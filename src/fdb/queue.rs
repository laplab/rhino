use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use foundationdb::FdbBindingError;
use ulid::Ulid;

use crate::{FdbTable, FdbValue, Predicate};

pub struct FdbQueue<'a> {
    pub(crate) table: FdbTable<'a>,
}

#[derive(Clone, Debug)]
pub struct LeaseId(Vec<FdbValue>);

pub enum FdbQueueError {
    LeaseExpired,
}

impl<'a> FdbQueue<'a> {
    pub fn enqueue(&self, values: impl IntoIterator<Item = (String, FdbValue)>) {
        let pk = vec![
            FdbValue::U64(get_now_ms()),
            // Ulids are used to deduplicate multiple items vesting at the same time.
            FdbValue::String(Ulid::new().to_string()),
        ];

        self.table.set(pk, values);
    }

    pub async fn dequeue(
        &self,
        lease_duration: Duration,
    ) -> Result<Option<(LeaseId, HashMap<String, FdbValue>)>, FdbBindingError> {
        // First, we try to find an item with expired vesting time.
        let partial_pk = vec![FdbValue::U64(get_now_ms())];
        let mut rows = self
            .table
            .get_first_n(Predicate::Less(partial_pk), 1)
            .await?;
        let (mut pk, values) = if rows.is_empty() {
            return Ok(None);
        } else {
            rows.pop().unwrap()
        };

        // Second, we update the item's vesting time. Since we are updating the primary key, we need
        // to first remove the record from the table and then insert it back.
        self.table.remove(pk.clone());

        pk[0] = FdbValue::U64(pk[0].clone().unwrap_u64() + lease_duration.as_millis() as u64);
        self.table.set(pk.clone(), values.clone());

        // Finally, we return the lease id. If the worker manages to finish its operation in
        // `lease_duration`, then it will be able to mark the item as completed using the lease id
        // provided. If not, another worker will pick up the task and update its vesting time,
        // meaning that this lease id will not match any item in the queue.
        Ok(Some((LeaseId(pk), values)))
    }

    pub async fn complete(
        &self,
        lease_id: LeaseId,
    ) -> Result<Result<(), FdbQueueError>, FdbBindingError> {
        match self.table.get(lease_id.0.clone()).await? {
            Some(_values) => {
                self.table.remove(lease_id.0);
                Ok(Ok(()))
            }
            None => Ok(Err(FdbQueueError::LeaseExpired)),
        }
    }
}

fn get_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock cannot drift to the time before unix epoch")
        .as_millis() as u64
}
