use std::{collections::HashMap, hash::Hash, sync::Arc};

use foundationdb::tenant;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    RwLock,
};

use mahogany::{
    api_server::{ClientRequest, ClientRequestPayload, ClientResponse, ClientResponsePayload},
    control_server, multiplexed_ws, recv_or_stop, region_server, regional_multiplexed_ws,
    run_ws_server, send_on_error, send_or_stop, ControlStreams, Region, RegionInfo,
    RegionalStreams, SerializableFdbValue,
};

struct ApiHandler {
    requests: UnboundedReceiver<ClientRequest>,
    responses: UnboundedSender<ClientResponse>,

    control_streams: ControlStreams,
    local_region: Region,
    local_streams: RegionalStreams,
    remote_streams: HashMap<Region, RegionalStreams>,

    tenant_id: Arc<RwLock<Option<String>>>,
}

impl ApiHandler {
    fn new(
        requests: UnboundedReceiver<ClientRequest>,
        responses: UnboundedSender<ClientResponse>,
        control_streams: ControlStreams,
        local_region: Region,
        local_streams: RegionalStreams,
        remote_streams: HashMap<Region, RegionalStreams>,
    ) -> Self {
        ApiHandler {
            requests,
            responses,
            control_streams,
            local_region,
            local_streams,
            remote_streams,
            tenant_id: Arc::new(RwLock::new(None)),
        }
    }

    fn state(&self, correlation_id: String) -> ApiHandlerState {
        ApiHandlerState {
            responses: self.responses.clone(),
            control_streams: self.control_streams.clone(),
            local_region: self.local_region,
            local_streams: self.local_streams.clone(),
            remote_streams: self.remote_streams.clone(),
            tenant_id: self.tenant_id.clone(),
            correlation_id,
        }
    }

    async fn handle(mut self) {
        // TODO: timeout unauthenticated requests.
        loop {
            let ClientRequest {
                payload,
                correlation_id,
            } = recv_or_stop!(self.requests);

            let state = self.state(correlation_id);

            use ClientRequestPayload::*;
            match payload {
                Login { token } => tokio::spawn(state.handle_login(token)),
                CreateShard { name, region } => {
                    tokio::spawn(state.handle_create_shard(name, region))
                }
                CreateTable { name, pk } => tokio::spawn(state.handle_create_table(name, pk)),
                SetRow {
                    shard_name,
                    table_name,
                    row,
                } => tokio::spawn(state.handle_set_row(shard_name, table_name, row)),
                GetRow {
                    shard_name,
                    table_name,
                    pk,
                } => tokio::spawn(state.handle_get_row(shard_name, table_name, pk)),
            };
        }
    }
}

struct ApiHandlerState {
    responses: UnboundedSender<ClientResponse>,

    control_streams: ControlStreams,
    local_region: Region,
    local_streams: RegionalStreams,
    remote_streams: HashMap<Region, RegionalStreams>,

    tenant_id: Arc<RwLock<Option<String>>>,

    correlation_id: String,
}

// TODO: `FdbError` is not always retryable. Check the error code before returning `PleaseRetry`.
impl ApiHandlerState {
    fn correlation_id(&self) -> String {
        self.correlation_id.clone()
    }

    fn retry_payload(&self, reason: impl ToString) -> ClientResponsePayload {
        ClientResponsePayload::PleaseRetry {
            reason: reason.to_string(),
        }
    }

    fn retry(&self, reason: impl ToString) -> ClientResponse {
        ClientResponse {
            payload: self.retry_payload(reason),
            correlation_id: self.correlation_id(),
        }
    }

    async fn handle_login(self, token: String) {
        let mut tenant_id = self.tenant_id.write().await;
        if tenant_id.is_some() {
            send_or_stop!(
                self.responses,
                ClientResponsePayload::AlreadyLoggedIn.correlate(self.correlation_id())
            );
        }

        // Login requests are always routed to the local region.
        let response = send_on_error!(
            async {
                let mut handler = self
                    .local_streams
                    .create_stream(self.correlation_id())
                    .await?;
                handler.send(
                    region_server::ClientRequestPayload::GetTenantIdByToken { token }
                        .correlate(self.correlation_id()),
                )?;
                handler.recv().await
            }
            .await,
            self.responses,
            self.retry(format!(
                "region {} is experiencing issues",
                self.local_region
            ))
        );

        use region_server::ClientResponsePayload::*;
        match response.payload {
            TenantIdByToken {
                tenant_id: fetched_tenant_id,
            } => {
                // Token matched to a valid tenant_id. Socket is now authenticated.
                *tenant_id = Some(fetched_tenant_id);
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::LoggedIn {
                        region: self.local_region,
                    }
                    .correlate(self.correlation_id())
                );
            }
            InvalidToken => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::InvalidToken.correlate(self.correlation_id())
                );
            }
            FdbError => {
                send_or_stop!(
                    self.responses,
                    self.retry(format!(
                        "region {} is experiencing issues",
                        self.local_region
                    ))
                );
            }
            _ => panic!("unexpected message type"),
        }
    }

    async fn handle_create_shard(self, shard_name: String, region: Region) {
        let tenant_id = match &*self.tenant_id.read().await {
            Some(tenant_id) => tenant_id.clone(),
            None => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::NotLoggedIn.correlate(self.correlation_id())
                );
                return;
            }
        };

        let response = send_on_error!(
            async {
                let mut handler = self
                    .control_streams
                    .create_stream(self.correlation_id())
                    .await?;
                handler.send(
                    control_server::ClientRequestPayload::AddShardToGlobalRouting {
                        tenant_id,
                        shard_name,
                        region,
                    }
                    .correlate(self.correlation_id()),
                )?;
                handler.recv().await
            }
            .await,
            self.responses,
            self.retry("metadata storage is experiencing issues")
        );

        use control_server::ClientResponsePayload::*;
        match response.payload {
            ShardAddedToGlobalRouting => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::ShardCreated.correlate(self.correlation_id())
                );
            }
            ShardRoutingAlreadyExists => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::ShardAlreadyExists.correlate(self.correlation_id())
                );
            }
            FdbError => {
                send_or_stop!(
                    self.responses,
                    self.retry("metadata storage is experiencing issues")
                );
            }
            _ => panic!("unexpected message type"),
        }
    }

    async fn handle_create_table(self, table_name: String, pk: Vec<String>) {
        let tenant_id = match &*self.tenant_id.read().await {
            Some(tenant_id) => tenant_id.clone(),
            None => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::NotLoggedIn.correlate(self.correlation_id())
                );
                return;
            }
        };

        let response = send_on_error!(
            async {
                let mut handler = self
                    .control_streams
                    .create_stream(self.correlation_id())
                    .await?;
                handler.send(
                    control_server::ClientRequestPayload::AddGlobalTableSchema {
                        tenant_id,
                        table_name,
                        pk,
                    }
                    .correlate(self.correlation_id()),
                )?;
                handler.recv().await
            }
            .await,
            self.responses,
            self.retry("metadata storage is experiencing issues")
        );

        use control_server::ClientResponsePayload::*;
        match response.payload {
            GlobalTableSchemaAdded => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::TableCreated.correlate(self.correlation_id())
                );
            }
            TableAlreadyExists => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::TableAlreadyExists.correlate(self.correlation_id())
                );
            }
            FdbError => {
                send_or_stop!(
                    self.responses,
                    self.retry("metadata storage is experiencing issues")
                );
            }
            _ => panic!("unexpected message type"),
        }
    }

    async fn set_to_region(
        self,
        tenant_id: String,
        region: Region,
        shard_name: String,
        table_name: String,
        row: HashMap<String, SerializableFdbValue>,
    ) {
        let streams = self.remote_streams.get(&region).unwrap();

        let set_resp = send_on_error!(
            async {
                let mut handler = streams.create_stream(self.correlation_id()).await?;
                handler.send(
                    region_server::ClientRequestPayload::SetRow {
                        tenant_id,
                        shard_name,
                        table_name,
                        row,
                    }
                    .correlate(self.correlation_id()),
                )?;
                handler.recv().await
            }
            .await,
            self.responses,
            self.retry(format!("region {} is experiencing issues", region))
        );

        let result_payload = match set_resp.payload {
            region_server::ClientResponsePayload::FdbError => {
                self.retry_payload(format!("region {} is experiencing issues", region))
            }
            region_server::ClientResponsePayload::SetRowCompleted => {
                ClientResponsePayload::SetRowCompleted
            }
            region_server::ClientResponsePayload::ShardNotFound => {
                ClientResponsePayload::ShardNotFound
            }
            region_server::ClientResponsePayload::TableNotFound => {
                ClientResponsePayload::TableNotFound
            }
            region_server::ClientResponsePayload::MissingPkValue { component } => {
                ClientResponsePayload::MissingPkValue { component }
            }
            _ => panic!("unexpected error"),
        };
        send_or_stop!(
            self.responses,
            result_payload.correlate(self.correlation_id())
        )
    }

    async fn try_get_shard_region_from_local(
        &self,
        tenant_id: String,
        shard_name: String,
    ) -> Result<region_server::ClientResponse, mahogany::WebsocketMultiplexerError> {
        async {
            let mut local_handler = self
                .local_streams
                .create_stream(self.correlation_id())
                .await?;
            local_handler.send(
                region_server::ClientRequestPayload::GetShardRegion {
                    tenant_id,
                    shard_name,
                }
                .correlate(self.correlation_id()),
            )?;
            local_handler.recv().await
        }
        .await
    }

    async fn try_get_shard_region_from_control(
        &self,
        tenant_id: String,
        shard_name: String,
    ) -> Result<control_server::ClientResponse, mahogany::WebsocketMultiplexerError> {
        async {
            let mut handler = self
                .control_streams
                .create_stream(self.correlation_id())
                .await?;
            handler.send(
                control_server::ClientRequestPayload::GetShardRegion {
                    tenant_id,
                    shard_name,
                }
                .correlate(self.correlation_id()),
            )?;
            handler.recv().await
        }
        .await
    }

    async fn handle_set_row(
        self,
        shard_name: String,
        table_name: String,
        row: HashMap<String, SerializableFdbValue>,
    ) {
        // Check if current connection is logged in.
        let tenant_id = match &*self.tenant_id.read().await {
            Some(tenant_id) => tenant_id.clone(),
            None => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::NotLoggedIn.correlate(self.correlation_id())
                );
                return;
            }
        };

        // Check local metadata first.
        let local_resp = send_on_error!(
            self.try_get_shard_region_from_local(tenant_id.clone(), shard_name.clone())
                .await,
            self.responses,
            self.retry(format!(
                "region {} is experiencing issues",
                self.local_region
            ))
        );

        {
            use region_server::ClientResponsePayload::*;
            match local_resp.payload {
                ShardLocated { region } => {
                    return self
                        .set_to_region(tenant_id, region, shard_name, table_name, row)
                        .await;
                }
                ShardNotFound => {}
                FdbError => {
                    send_or_stop!(
                        self.responses,
                        self.retry(format!(
                            "region {} is experiencing issues",
                            self.local_region
                        ))
                    )
                }
                resp => {
                    panic!(
                        "unexpected message type: {}",
                        serde_json::to_string(&resp).unwrap_or("unknown".to_string())
                    )
                }
            }
        }

        // Go to global metadata storage.
        let shard_region = send_on_error!(
            self.try_get_shard_region_from_control(tenant_id.clone(), shard_name.clone())
                .await,
            self.responses,
            self.retry("metadata storage is experiencing issues")
        );

        use control_server::ClientResponsePayload::*;
        match shard_region.payload {
            ShardLocated { region } => {
                self.set_to_region(tenant_id, region, shard_name, table_name, row)
                    .await
            }
            ShardNotFound => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::ShardNotFound.correlate(self.correlation_id())
                )
            }
            FdbError => {
                send_or_stop!(
                    self.responses,
                    self.retry("metadata storage is experiencing issues")
                );
            }
            resp => panic!(
                "unexpected message type: {}",
                serde_json::to_string(&resp).unwrap_or("unknown".to_string())
            ),
        }
    }

    async fn get_from_region(
        &self,
        tenant_id: String,
        region: Region,
        shard_name: String,
        table_name: String,
        pk: HashMap<String, SerializableFdbValue>,
    ) {
        let streams = self.remote_streams.get(&region).unwrap();

        let get_resp = send_on_error!(
            async {
                let mut handler = streams.create_stream(self.correlation_id()).await?;
                handler.send(
                    region_server::ClientRequestPayload::GetRow {
                        tenant_id,
                        shard_name,
                        table_name,
                        pk,
                    }
                    .correlate(self.correlation_id()),
                )?;
                handler.recv().await
            }
            .await,
            self.responses,
            self.retry("metadata storage is experiencing issues")
        );

        use region_server::ClientResponsePayload::*;
        let result_payload = match get_resp.payload {
            FdbError => self.retry_payload(format!("region {} is experiencing issues", region)),
            RowFound { row } => ClientResponsePayload::RowFound { row: row },
            RowNotFound => ClientResponsePayload::RowNotFound,
            ShardNotFound => ClientResponsePayload::ShardNotFound,

            MissingPkValue { component } => ClientResponsePayload::MissingPkValue { component },
            resp => panic!(
                "unexpected error: [{}]",
                serde_json::to_string(&resp).unwrap_or("unknown".to_string())
            ),
        };
        send_or_stop!(
            self.responses,
            result_payload.correlate(self.correlation_id())
        )
    }

    async fn handle_get_row(
        self,
        shard_name: String,
        table_name: String,
        pk: HashMap<String, SerializableFdbValue>,
    ) {
        // Check if current connection is logged in.
        let tenant_id = match &*self.tenant_id.read().await {
            Some(tenant_id) => tenant_id.clone(),
            None => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::NotLoggedIn.correlate(self.correlation_id())
                );
                return;
            }
        };

        // Check local metadata first.
        let local_resp = send_on_error!(
            self.try_get_shard_region_from_local(tenant_id.clone(), shard_name.clone())
                .await,
            self.responses,
            self.retry(format!(
                "region {} is experiencing issues",
                self.local_region
            ))
        );

        {
            use region_server::ClientResponsePayload::*;
            match local_resp.payload {
                ShardLocated { region } => {
                    return self
                        .get_from_region(tenant_id, region, shard_name, table_name, pk)
                        .await;
                }
                ShardNotFound => {}
                FdbError => {
                    send_or_stop!(
                        self.responses,
                        self.retry("Error from {region} server storage")
                    )
                }
                resp => {
                    panic!(
                        "unexpected message type: {}",
                        serde_json::to_string(&resp).unwrap_or("unknown".to_string())
                    )
                }
            }
        }

        // Go to global metadata storage.
        let shard_region = send_on_error!(
            self.try_get_shard_region_from_control(tenant_id.clone(), shard_name.clone())
                .await,
            self.responses,
            self.retry("metadata storage is experiencing issues")
        );

        use control_server::ClientResponsePayload::*;
        match shard_region.payload {
            ShardLocated { region } => {
                self.get_from_region(tenant_id, region, shard_name, table_name, pk)
                    .await
            }
            ShardNotFound => {
                send_or_stop!(
                    self.responses,
                    ClientResponsePayload::ShardNotFound.correlate(self.correlation_id())
                )
            }
            FdbError => {
                send_or_stop!(
                    self.responses,
                    self.retry("metadata storage is experiencing issues")
                );
            }
            resp => panic!(
                "unexpected message type: {}",
                serde_json::to_string(&resp).unwrap_or("unknown".to_string())
            ),
        }
    }
}

pub async fn run(
    address: String,
    control_address: String,
    local_region: Region,
    regions: Vec<RegionInfo>,
) {
    let control_streams = multiplexed_ws::<
        control_server::ClientResponse,
        control_server::ClientRequest,
    >(Region::Control, control_address);

    // TODO: Somehow solve the problem of a noisy neighbour.
    // Establish connections to other regions.
    let region_streams = regional_multiplexed_ws(regions);
    let local_region_streams = if let Some(local_region) = region_streams.get(&local_region) {
        local_region.clone()
    } else {
        panic!("local region is missing from the regions list");
    };

    run_ws_server(&address, |requests, responses| {
        ApiHandler::new(
            requests,
            responses,
            control_streams.clone(),
            local_region,
            local_region_streams.clone(),
            region_streams.clone(),
        )
        .handle()
    })
    .await;

    for (_, streams) in region_streams {
        streams.stop().await;
    }
}
