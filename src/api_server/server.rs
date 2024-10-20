use std::{collections::HashMap, sync::Arc};

use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    RwLock,
};

use mahogany::{
    api_server::{ClientRequest, ClientRequestPayload, ClientResponse, ClientResponsePayload},
    control_server, multiplexed_ws, recv_or_stop, region_server, regional_multiplexed_ws,
    run_ws_server, send_on_error, send_or_stop, ControlStreams, Region, RegionInfo,
    RegionalStreams,
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

    fn retry(&self, reason: impl ToString) -> ClientResponse {
        ClientResponse {
            payload: ClientResponsePayload::PleaseRetry {
                reason: reason.to_string(),
            },
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
