use std::collections::HashMap;

use foundationdb::{
    directory::DirectoryOutput,
    options::StreamingMode,
    tuple::{TuplePack, TupleUnpack},
    FdbBindingError, KeySelector, RangeOption, RetryableTransaction,
};

use crate::FdbValue;

pub struct FdbTable<'a> {
    pub(crate) tx: &'a RetryableTransaction,
    pub(crate) dir: foundationdb::directory::DirectoryOutput,
}

pub enum Predicate {
    Less(Vec<FdbValue>),
}

impl<'a> FdbTable<'a> {
    pub fn set(
        &self,
        mut primary_key: Vec<FdbValue>,
        values: impl IntoIterator<Item = (String, FdbValue)>,
    ) {
        for (column, value) in values {
            primary_key.push(FdbValue::String(column));

            let encoded_key = pack_key_in_dir(&self.dir, &primary_key);
            let encoded_value = foundationdb::tuple::pack(&value);

            self.tx.set(&encoded_key, &encoded_value);

            primary_key.pop().expect("just pushed");
        }
    }

    pub fn remove(&self, primary_key: Vec<FdbValue>) {
        let encoded_key = pack_key_in_dir(&self.dir, &primary_key);
        let (begin, end) = range_starts_with(encoded_key);
        self.tx.clear_range(begin.key(), end.key());
    }

    pub async fn get(
        &self,
        primary_key: Vec<FdbValue>,
    ) -> Result<Option<HashMap<String, FdbValue>>, FdbBindingError> {
        let encoded_key = pack_key_in_dir(&self.dir, &primary_key);

        let (begin, end) = range_starts_with(encoded_key);
        let mut range = RangeOption {
            begin,
            end,
            mode: StreamingMode::WantAll,
            ..RangeOption::default()
        };

        // There is zero public documentation saying that this is how you are supposed to do ranged
        // reads. This code is ported from this section of FDB testing code:
        // https://github.com/apple/foundationdb/blob/8e099d276d30d0721077cd75a3a3f0d746e7ea58/bindings/c/test/apitester/TesterCorrectnessWorkload.cpp#L206-L229
        let mut result = HashMap::default();
        loop {
            // TODO: consider snapshot read.
            let values = self
                .tx
                .get_range(&range, 0, false)
                .await
                .map_err(FdbBindingError::from)?;
            let has_more = values.more();

            let len = values.len();
            for (index, kv) in values.into_iter().enumerate() {
                if has_more && index + 1 == len {
                    range.begin = KeySelector::first_greater_than(kv.key().to_vec());
                }

                let mut pk_and_column: Vec<FdbValue> = unpack_key_in_dir(&self.dir, kv.key());
                let column = pk_and_column
                    .pop()
                    .expect("primary key must have at least 1 element")
                    .unwrap_string();

                let value = foundationdb::tuple::unpack(kv.value()).expect("invalid encoding");

                result.insert(column, value);
            }

            if !has_more {
                break;
            }
        }

        Ok(if result.is_empty() {
            None
        } else {
            Some(result)
        })
    }

    pub async fn get_one_column(
        &self,
        mut primary_key: Vec<FdbValue>,
        column: String,
    ) -> Result<Option<FdbValue>, FdbBindingError> {
        primary_key.push(FdbValue::String(column));
        let encoded_key = pack_key_in_dir(&self.dir, &primary_key);

        // TODO: consider snapshot read.
        Ok(self
            .tx
            .get(&encoded_key, false)
            .await?
            .map(|slice| foundationdb::tuple::unpack(&slice).expect("invalid encoding")))
    }

    pub async fn get_first_n(
        &self,
        pk_predicate: Predicate,
        limit: usize,
    ) -> Result<Vec<(Vec<FdbValue>, HashMap<String, FdbValue>)>, FdbBindingError> {
        let mut range = match &pk_predicate {
            Predicate::Less(pk) => {
                let empty_array: [FdbValue; 0] = Default::default();
                let start_key = pack_key_in_dir(&self.dir, &empty_array.as_slice());
                let end_key = pack_key_in_dir(&self.dir, pk);
                RangeOption {
                    begin: KeySelector::first_greater_than(start_key),
                    end: KeySelector::first_greater_or_equal(end_key),
                    mode: StreamingMode::Iterator,
                    reverse: true,
                    ..RangeOption::default()
                }
            }
        };

        let mut iteration = 0;
        let mut rows = vec![];
        let mut current_pk = None;
        let mut current_row = HashMap::default();
        'range_loop: loop {
            // TODO: consider snapshot read.
            iteration += 1;
            let values = self
                .tx
                .get_range(&range, iteration, false)
                .await
                .map_err(FdbBindingError::from)?;
            let has_more = values.more();

            let len = values.len();
            for (index, kv) in values.into_iter().enumerate() {
                if has_more && index + 1 == len {
                    let last_read_key = kv.key().to_vec();
                    if range.reverse {
                        // Note: the end of the range is never included.
                        range.end = KeySelector::first_greater_or_equal(last_read_key);
                    } else {
                        range.begin = KeySelector::first_greater_than(last_read_key);
                    }
                }

                let (pk, column) = {
                    let mut pk_and_column: Vec<FdbValue> = unpack_key_in_dir(&self.dir, kv.key());
                    let column = pk_and_column
                        .pop()
                        .expect("primary key must have at least 1 element")
                        .unwrap_string();
                    (pk_and_column, column)
                };

                match &mut current_pk {
                    Some(current_pk) => {
                        if *current_pk != pk {
                            rows.push((
                                std::mem::replace(current_pk, pk),
                                std::mem::take(&mut current_row),
                            ));

                            if rows.len() == limit {
                                break 'range_loop;
                            }
                        }
                    }
                    None => current_pk = Some(pk),
                }

                let value = foundationdb::tuple::unpack(kv.value()).expect("invalid encoding");

                current_row.insert(column, value);
            }

            if !has_more {
                break 'range_loop;
            }
        }

        if rows.len() < limit && current_pk.is_some() {
            rows.push((current_pk.unwrap(), current_row));
        }

        Ok(rows)
    }
}

fn pack_key_in_dir<T: TuplePack>(dir: &DirectoryOutput, key: &T) -> Vec<u8> {
    dir.pack(key)
        .expect("partition layer is not used in the app")
}

fn unpack_key_in_dir<'a, T: TupleUnpack<'a>>(dir: &DirectoryOutput, key: &'a [u8]) -> T {
    dir.unpack(key)
        .expect("partition layer is not used in the app")
        .expect("invalid encoding")
}

fn range_starts_with(prefix: Vec<u8>) -> (KeySelector<'static>, KeySelector<'static>) {
    let start_key = prefix;

    // This code is ported from official Python SDK, see here:
    // https://github.com/apple/foundationdb/blob/324438cf26d372bedb8ca6b881877bf62dbdfb88/bindings/python/fdb/impl.py#L1693-L1698
    // It is later used to implement prefix read routine here:
    // https://github.com/apple/foundationdb/blob/324438cf26d372bedb8ca6b881877bf62dbdfb88/bindings/python/fdb/impl.py#L444-L446
    let mut end_key = start_key.clone();
    while end_key.ends_with(&[u8::MAX]) {
        end_key.pop();
    }
    let last_byte = end_key
        .last_mut()
        .expect("key must contain one byte not equal to 255");
    *last_byte = last_byte
        .checked_add(1)
        .expect("just checked that the last byte isn't 255");

    (
        KeySelector::first_greater_or_equal(start_key),
        KeySelector::first_greater_or_equal(end_key),
    )
}
