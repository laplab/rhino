use std::collections::HashMap;

use foundationdb::{FdbBindingError, RetryableTransaction};

use crate::{FdbClient, FdbTable, FdbValue, SchemaManager};

pub struct RhinoTable<'a> {
    table: FdbTable<'a>,
    pk: Vec<String>,
}

pub enum RhinoTableError {
    TableDoesNotExist,
    MissingPrimaryKeyComponent { component: String },
}

const TENANTS_DIR: &str = "tenants";

fn tenant_table_path(tenant_id: &str, shard_name: &str, table_name: &str) -> [String; 5] {
    [
        TENANTS_DIR.into(),
        tenant_id.into(),
        String::from("table"),
        shard_name.into(),
        table_name.into(),
    ]
}

impl<'a> RhinoTable<'a> {
    pub async fn new(
        db: &FdbClient,
        tx: &'a RetryableTransaction,
        tenant_id: &str,
        shard_name: &str,
        table_name: &str,
    ) -> Result<Result<Self, RhinoTableError>, FdbBindingError> {
        let schema = SchemaManager::new(db, tx).await?;
        let pk = match schema.get_table_pk(tenant_id, table_name).await? {
            Some(pk) => pk,
            None => return Ok(Err(RhinoTableError::TableDoesNotExist)),
        };

        let table = db
            .open_table(tx, &tenant_table_path(tenant_id, shard_name, table_name))
            .await?;

        Ok(Ok(Self { table, pk }))
    }

    fn extract_pk(
        &self,
        row: &HashMap<String, FdbValue>,
    ) -> Result<Vec<FdbValue>, RhinoTableError> {
        let mut pk_values = Vec::with_capacity(self.pk.len());
        for key in &self.pk {
            match row.get(key) {
                Some(value) => pk_values.push(value.clone()),
                None => {
                    return Err(RhinoTableError::MissingPrimaryKeyComponent {
                        component: key.clone(),
                    });
                }
            }
        }
        Ok(pk_values)
    }

    pub async fn set(
        &self,
        row: HashMap<String, FdbValue>,
    ) -> Result<Result<(), RhinoTableError>, FdbBindingError> {
        let pk_values = match self.extract_pk(&row) {
            Ok(pk_values) => pk_values,
            Err(err) => return Ok(Err(err)),
        };
        self.table.set(pk_values, row.into_iter());
        Ok(Ok(()))
    }

    pub async fn get(
        &self,
        pk: HashMap<String, FdbValue>,
    ) -> Result<Result<Option<HashMap<String, FdbValue>>, RhinoTableError>, FdbBindingError> {
        let pk_values = match self.extract_pk(&pk) {
            Ok(pk_values) => pk_values,
            Err(err) => return Ok(Err(err)),
        };

        Ok(Ok(self.table.get(pk_values).await?))
    }
}
