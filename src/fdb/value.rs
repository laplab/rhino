use foundationdb::tuple::{PackResult, TupleDepth, TuplePack, TupleUnpack, VersionstampOffset};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FdbValue {
    String(String),
    U64(u64),
}

type TypeDiscriminator = u16;

impl FdbValue {
    const STRING_DISC: TypeDiscriminator = 1;
    const U64_DISC: TypeDiscriminator = 2;

    pub fn unwrap_string(self) -> String {
        match self {
            Self::String(value) => value,
            _ => panic!("unexpected type"),
        }
    }

    pub fn unwrap_u64(self) -> u64 {
        match self {
            Self::U64(value) => value,
            _ => panic!("unexpected type"),
        }
    }
}

impl TuplePack for FdbValue {
    fn pack<W: std::io::Write>(
        &self,
        w: &mut W,
        tuple_depth: TupleDepth,
    ) -> std::io::Result<VersionstampOffset> {
        use FdbValue::*;
        match self {
            String(value) => (FdbValue::STRING_DISC, value).pack(w, tuple_depth),
            U64(value) => (FdbValue::U64_DISC, value).pack(w, tuple_depth),
        }
    }
}

// The code in this module is copied from foundationdb-rs directly.
mod foundationdb_rs_priv {
    use foundationdb::tuple::{PackError, PackResult};

    pub const NESTED: u8 = 0x05;
    pub const NIL: u8 = 0x00;

    #[inline]
    pub fn parse_byte(input: &[u8]) -> PackResult<(&[u8], u8)> {
        if input.is_empty() {
            Err(PackError::MissingBytes)
        } else {
            Ok((&input[1..], input[0]))
        }
    }

    pub fn parse_code(input: &[u8], expected: u8) -> PackResult<&[u8]> {
        let (input, found) = parse_byte(input)?;
        if found == expected {
            Ok(input)
        } else {
            Err(PackError::BadCode {
                found,
                expected: Some(expected),
            })
        }
    }
}

impl<'de> TupleUnpack<'de> for FdbValue {
    fn unpack(input: &'de [u8], tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)> {
        // We can't just unpack a tuple, because we don't know the type of the second element of the
        // tuple in advance. The code below is a modified version of the tuple unpacking code in
        // foundationdb-rs.
        use foundationdb_rs_priv::*;

        let input = if tuple_depth.depth() > 0 {
            parse_code(input, NESTED)?
        } else {
            input
        };
        let nested_tuple_depth = tuple_depth.increment();

        let (input, disc) = TypeDiscriminator::unpack(input, nested_tuple_depth)?;

        let (input, value) = match disc {
            FdbValue::STRING_DISC => {
                let (input, value) = String::unpack(input, nested_tuple_depth)?;
                (input, FdbValue::String(value))
            }
            FdbValue::U64_DISC => {
                let (input, value) = u64::unpack(input, nested_tuple_depth)?;
                (input, FdbValue::U64(value))
            }
            _ => panic!("invalid discriminator"),
        };

        let input = if tuple_depth.depth() > 0 {
            parse_code(input, NIL)?
        } else {
            input
        };

        Ok((input, value))
    }
}
