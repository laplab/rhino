use foundationdb::{
    api::NetworkAutoStop, directory::Directory, FdbBindingError, RetryableTransaction,
};
use futures_util::Future;
use tracing::info;

use crate::FdbQueue;

use super::table::FdbTable;

pub struct FdbClient {
    db: foundationdb::Database,
    root_dir: foundationdb::directory::DirectoryLayer,
    // Note: It is important for the `network` field to be the last to ensure
    // that it is dropped the last.
    _network: NetworkAutoStop,
}

impl FdbClient {
    pub fn new(cluster_file_path: &str) -> Self {
        let network = unsafe { foundationdb::boot() };
        let db = foundationdb::Database::from_path(cluster_file_path).expect("failed to open FDB");
        let root_dir = foundationdb::directory::DirectoryLayer::default();
        // TODO: Set retry limit and timeout on transactions.

        Self {
            db,
            root_dir,
            _network: network,
        }
    }

    pub async fn run<F, Fut, T>(&self, closure: F) -> Result<T, FdbBindingError>
    where
        F: Fn(RetryableTransaction) -> Fut,
        Fut: Future<Output = Result<T, FdbBindingError>>,
    {
        self.db.run(move |trx, _maybe_committed| closure(trx)).await
    }

    pub async fn open_table<'a>(
        &self,
        tx: &'a RetryableTransaction,
        path: &[String],
    ) -> Result<FdbTable<'a>, FdbBindingError> {
        let dir = self
            .root_dir
            .create_or_open(&tx, path, None, None)
            .await
            .map_err(FdbBindingError::from)?;

        Ok(FdbTable { tx, dir })
    }

    pub async fn open_queue<'a>(
        &self,
        tx: &'a RetryableTransaction,
        path: &[String],
    ) -> Result<FdbQueue<'a>, FdbBindingError> {
        Ok(FdbQueue {
            table: self.open_table(tx, path).await?,
        })
    }
}
