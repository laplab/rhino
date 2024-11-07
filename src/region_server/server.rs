use std::{collections::HashMap, sync::Arc};

use rhino::{
    recv_or_stop,
    region_server::{ClientRequest, ClientRequestPayload, ClientResponse, ClientResponsePayload},
    run_ws_server, send_or_stop, AuthManager, FdbClient, FdbValue, Region, RhinoTable,
    RhinoTableError, RoutingManager, SchemaManager, SerializableFdbValue,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::error;

struct RegionHandler {
    requests: UnboundedReceiver<ClientRequest>,
    responses: UnboundedSender<ClientResponse>,
    db: Arc<FdbClient>,
    region: Region,
}

impl RegionHandler {
    fn new(
        requests: UnboundedReceiver<ClientRequest>,
        responses: UnboundedSender<ClientResponse>,
        db: Arc<FdbClient>,
        region: Region,
    ) -> Self {
        RegionHandler {
            requests,
            responses,
            db,
            region,
        }
    }

    fn state(&self, correlation_id: String) -> RegionHandlerState {
        RegionHandlerState {
            responses: self.responses.clone(),
            db: self.db.clone(),
            region: self.region,
            correlation_id,
        }
    }

    async fn handle(mut self) {
        loop {
            let ClientRequest {
                payload,
                correlation_id,
            } = recv_or_stop!(self.requests);

            let state = self.state(correlation_id);

            use ClientRequestPayload::*;
            match payload {
                GetTenantIdByToken { token } => {
                    tokio::spawn(state.handle_get_tenant_id_by_token(token))
                }
                GetShardRegion {
                    tenant_id,
                    shard_name,
                } => tokio::spawn(state.handle_get_shard_region(tenant_id, shard_name)),
                GetTableSchema {
                    tenant_id,
                    table_name,
                } => tokio::spawn(state.handle_get_table_schema(tenant_id, table_name)),

                AddLocalTenant { tenant_id, token } => {
                    tokio::spawn(state.handle_add_local_tenant(tenant_id, token))
                }
                AddShardToLocalRouting {
                    tenant_id,
                    shard_name,
                    region,
                } => tokio::spawn(
                    state.handle_add_shard_to_local_routing(tenant_id, shard_name, region),
                ),
                AddLocalTableSchema {
                    tenant_id,
                    table_name,
                    pk,
                } => tokio::spawn(state.handle_add_local_table_schema(tenant_id, table_name, pk)),
                SetRow {
                    tenant_id,
                    shard_name,
                    table_name,
                    row,
                } => tokio::spawn(state.handle_set_row(tenant_id, shard_name, table_name, row)),
                GetRow {
                    tenant_id,
                    shard_name,
                    table_name,
                    pk,
                } => tokio::spawn(state.handle_get_row(tenant_id, shard_name, table_name, pk)),
            };
        }
    }
}

struct RegionHandlerState {
    responses: UnboundedSender<ClientResponse>,
    db: Arc<FdbClient>,
    region: Region,

    correlation_id: String,
}

impl RegionHandlerState {
    async fn handle_get_tenant_id_by_token(self, token: String) {
        // TODO: check token length.
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let token = &token;
                async move {
                    let auth = AuthManager::new(db, &tx).await?;
                    auth.get_tenant_id_by_token(token).await
                }
            })
            .await;

        let response = match result {
            Ok(Some(tenant_id)) => ClientResponsePayload::TenantIdByToken { tenant_id },
            Ok(None) => ClientResponsePayload::InvalidToken,
            Err(err) => {
                error!(?err, "failed to fetch tenant_id by token");
                ClientResponsePayload::FdbError
            }
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_add_local_tenant(self, tenant_id: String, token: String) {
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let tenant_id = &tenant_id;
                let token = &token;
                async move {
                    let auth = AuthManager::new(db, &tx).await?;
                    auth.add_tenant(tenant_id, token);
                    Ok(())
                }
            })
            .await;

        let response = match result {
            Ok(..) => ClientResponsePayload::LocalTenantAdded,
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_get_shard_region(self, tenant_id: String, shard_name: String) {
        // TODO: check token length.
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let tenant_id = &tenant_id;
                let shard_name = &shard_name;
                async move {
                    let routing = RoutingManager::new(db, &tx).await?;
                    routing.get_shard_region(tenant_id, shard_name).await
                }
            })
            .await;

        let response = match result {
            Ok(Some(region)) => ClientResponsePayload::ShardLocated { region },
            Ok(None) => ClientResponsePayload::ShardNotFound,
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_get_table_schema(self, tenant_id: String, table_name: String) {
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let tenant_id = &tenant_id;
                let table_name = &table_name;
                async move {
                    let schema = SchemaManager::new(db, &tx).await?;
                    schema.get_table_pk(tenant_id, &table_name).await
                }
            })
            .await;

        let response = match result {
            Ok(Some(pk)) => ClientResponsePayload::TableSchema { pk },
            Ok(None) => ClientResponsePayload::TableNotFound,
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_add_shard_to_local_routing(
        self,
        tenant_id: String,
        shard_name: String,
        shard_region: Region,
    ) {
        // TODO: check shard name length.
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let tenant_id = &tenant_id;
                let shard_name = &shard_name;
                async move {
                    let routing = RoutingManager::new(db, &tx).await?;
                    routing
                        .add_shard(&tenant_id, &shard_name, shard_region, true)
                        .await
                }
            })
            .await;

        let response = match result {
            Ok(..) => ClientResponsePayload::ShardAddedToLocalRouting,
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_add_local_table_schema(
        self,
        tenant_id: String,
        table_name: String,
        pk: Vec<String>,
    ) {
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let tenant_id = &tenant_id;
                let table_name = &table_name;
                let pk = &pk;
                async move {
                    let schema = SchemaManager::new(db, &tx).await?;
                    schema
                        .add_table(&tenant_id, &table_name, pk.clone(), true)
                        .await
                }
            })
            .await;

        let response = match result {
            Ok(..) => ClientResponsePayload::LocalTableSchemaAdded,
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_set_row(
        self,
        tenant_id: String,
        shard_name: String,
        table_name: String,
        row: HashMap<String, SerializableFdbValue>,
    ) {
        let row: HashMap<String, FdbValue> = row
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect();
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let tenant_id = &tenant_id;
                let shard_name = &shard_name;
                let table_name = &table_name;
                let row = &row;
                async move {
                    let table =
                        match RhinoTable::new(db, &tx, tenant_id, shard_name, table_name).await? {
                            Ok(table) => table,
                            Err(err) => return Ok(Err(err)),
                        };
                    table.set(row.clone()).await
                }
            })
            .await;

        let response = match result {
            Ok(Ok(())) => ClientResponsePayload::SetRowCompleted,
            Ok(Err(RhinoTableError::TableDoesNotExist)) => ClientResponsePayload::TableNotFound,
            Ok(Err(RhinoTableError::MissingPrimaryKeyComponent { component })) => {
                ClientResponsePayload::MissingPkValue { component }
            }
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_get_row(
        self,
        tenant_id: String,
        shard_name: String,
        table_name: String,
        pk: HashMap<String, SerializableFdbValue>,
    ) {
        let pk: HashMap<String, FdbValue> = pk
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect();
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let tenant_id = &tenant_id;
                let shard_name = &shard_name;
                let table_name = &table_name;
                let pk = &pk;
                async move {
                    let table =
                        match RhinoTable::new(db, &tx, tenant_id, shard_name, table_name).await? {
                            Ok(table) => table,
                            Err(err) => return Ok(Err(err)),
                        };
                    table.get(pk.clone()).await
                }
            })
            .await;

        let response = match result {
            Ok(Ok(Some(row))) => {
                let row = row
                    .into_iter()
                    .map(|(key, value)| (key, value.into()))
                    .collect();
                ClientResponsePayload::RowFound { row }
            }
            Ok(Ok(None)) => ClientResponsePayload::RowNotFound,
            Ok(Err(RhinoTableError::TableDoesNotExist)) => ClientResponsePayload::TableNotFound,
            Ok(Err(RhinoTableError::MissingPrimaryKeyComponent { component })) => {
                ClientResponsePayload::MissingPkValue { component }
            }
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }
}

pub async fn run(address: String, db: Arc<FdbClient>, region: Region) {
    run_ws_server(&address, move |requests, responses| {
        RegionHandler::new(requests, responses, db.clone(), region).handle()
    })
    .await;
}
