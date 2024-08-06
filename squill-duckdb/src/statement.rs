use arrow_array::RecordBatch;
use squill_core::driver::{DriverStatement, Result};
use squill_core::parameters::Parameters;
use squill_core::Error;
use std::{cell::RefCell, rc::Rc};

#[derive(Clone)]
pub(crate) struct DuckDBStatement<'c> {
    pub(crate) inner: Rc<RefCell<duckdb::Statement<'c>>>,
}

impl DriverStatement for DuckDBStatement<'_> {
    fn bind(&mut self, parameters: Parameters) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let expected = inner.parameter_count();
        match parameters {
            Parameters::None => {
                if expected > 0 {
                    return Err(Error::InvalidParameterCount { expected, actual: 0 }.into());
                }
                Ok(())
            }
            Parameters::Positional(values) => {
                if expected != values.len() {
                    return Err(Error::InvalidParameterCount { expected, actual: values.len() }.into());
                }
                // The valid values for the index `in raw_bind_parameter` begin at `1`, and end at
                // [`Statement::parameter_count`], inclusive.
                for (index, value) in values.iter().enumerate() {
                    inner.raw_bind_parameter(index + 1, crate::values::Adapter(value))?;
                }
                Ok(())
            }
        }
    }

    fn execute(&mut self) -> Result<u64> {
        match self.inner.borrow_mut().raw_execute() {
            Ok(affected_rows) => Ok(affected_rows as u64),
            Err(error) => Err(error.into()),
        }
    }

    fn query<'s>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        match self.inner.borrow_mut().raw_execute() {
            Ok(_affected_rows) => Ok(Box::new(self.clone())),
            Err(error) => Err(error.into()),
        }
    }
}

/// Iterator over the record batches.
///
/// This iterator is used to iterate over the record batches returned by the query.
/// Because with DuckDB we are directly accessing the record batches from the statement execution, we don't need to
/// expose the Row level as we would do with other drivers. This is why this iterator is also the statement itself and
/// why {@link DuckDBStatement::query} returns a clone of itself as the iterator.
impl<'conn> Iterator for DuckDBStatement<'conn> {
    type Item = Result<arrow_array::RecordBatch>;

    fn next(&mut self) -> Option<Result<arrow_array::RecordBatch>> {
        let step = self.inner.borrow().step();
        if step.is_some() {
            let batch_record = RecordBatch::from(step.unwrap());
            Some(Ok(batch_record))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::IN_MEMORY_URI;
    use arrow_array::Array;
    use chrono::NaiveTime;
    use rust_decimal::Decimal;
    use squill_core::connection::Connection;
    use squill_core::decode::Decode;
    use squill_core::query_arrow;
    use squill_core::values::{TimeUnit, Value};
    use uuid::Uuid;

    #[test]
    fn test_binding_primitive_types() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();

        // Note: u128 is intentionally omitted because DuckDB does not support UInt128 yet for some reason.
        // https://github.com/duckdb/duckdb-rs/issues/273
        let mut stmt = conn
            .prepare(
                r#"SELECT 
              /*  0 */ ?::BOOLEAN,
              /*  1 */ ?::TINYINT,
              /*  2 */ ?::SMALLINT,
              /*  3 */ ?::INTEGER,
              /*  4 */ ?::BIGINT,
              /*  5 */ ?::HUGEINT,
              /*  6 */ ?::UTINYINT,
              /*  7 */ ?::USMALLINT,
              /*  8 */ ?::UINTEGER,
              /*  9 */ ?::UBIGINT,
              /* 10 */ 0, /* placeholder for ?::UHUGEINT */
              /* 11 */ ?::FLOAT,
              /* 12 */ ?::DOUBLE,
              /* 13 */ ?::VARCHAR(100),
              /* 14 */ ?::BLOB
              "#,
            )
            .unwrap();

        let batch = query_arrow!(
            stmt,
            /*  0 */ true,
            /*  1 */ i8::MAX,
            /*  2 */ i16::MAX,
            /*  3 */ i32::MAX,
            /*  4 */ i64::MAX,
            /*  5 */ i128::MAX,
            /*  6 */ u8::MAX,
            /*  7 */ u16::MAX,
            /*  8 */ u32::MAX,
            /*  9 */ u64::MAX,
            /* 10 */ /* u128::MAX, */
            /* 11 */ f32::MAX,
            /* 12 */ f64::MAX,
            /* 13 */ "Hello, World!",
            /* 14 */ vec![0xde, 0xad, 0xbe, 0xef]
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

        // // u128 is not tested because DuckDB does not support UHUGEINT yet for some reason.
        assert!(bool::decode(batch.column(0), 0)); // 0 - BOOLEAN
        assert_eq!(i8::decode(batch.column(1), 0), i8::MAX); // 1 - TINYINT
        assert_eq!(i16::decode(batch.column(2), 0), i16::MAX); // 2 - SMALLINT
        assert_eq!(i32::decode(batch.column(3), 0), i32::MAX); // 3 - INTEGER
        assert_eq!(i64::decode(batch.column(4), 0), i64::MAX); // 4 - BIGINT
        assert_eq!(i128::decode(batch.column(5), 0), i128::MAX); // 5 - HUGEINT
        assert_eq!(u8::decode(batch.column(6), 0), u8::MAX); // 6 - UTINYINT
        assert_eq!(u16::decode(batch.column(7), 0), u16::MAX); // 7 - USMALLINT
        assert_eq!(u32::decode(batch.column(8), 0), u32::MAX); // 8 - UINTEGER
        assert_eq!(u64::decode(batch.column(9), 0), u64::MAX); // 9 - UBIGINT
        assert_eq!(f32::decode(batch.column(11), 0), f32::MAX); // 11 - FLOAT
        assert_eq!(f64::decode(batch.column(12), 0), f64::MAX); // 12 - DOUBLE
        assert_eq!(String::decode(batch.column(13), 0), "Hello, World!"); // 13 - VARCHAR
        assert_eq!(Vec::<u8>::decode(batch.column(14), 0), vec![0xde, 0xad, 0xbe, 0xef]);
        // 14 - BLOB
    }

    #[test]
    fn test_binding_datetime_types() {
        use chrono::{DateTime, Utc};

        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        let mut stmt = conn
            .prepare(
                r#"SELECT 
          /* 0 */ '2014/11/01'::DATE,
          /* 1 */ ?::DATE,
          /* 2 */ '2024-07-03 08:56:05.716022-07'::TIMESTAMP WITH TIME ZONE,
          /* 3 */ ?::TIMESTAMP WITH TIME ZONE,
          /* 4 */ '2024-07-03 08:56:05.716022'::TIMESTAMP,
          /* 5 */ ?::TIMESTAMP,
          /* 6 */ '11:30:00.123456'::TIME,
          /* 7 */ ?::TIME,
          /* 8 */ '1 year 5 days 12 mins 13 seconds 8 microseconds'::INTERVAL,
          /* 9 */ ?::INTERVAL
          "#,
            )
            .unwrap();
        let batch = query_arrow!(
            stmt,
            /* 0 - binding for DATE tested on #1 */
            /* 1 */ Value::Date32(16375),
            /* 2 - binding for TIMESTAMP WITH TIME ZONE tested on #3 */
            /* 3 */
            DateTime::parse_from_rfc3339("2024-07-03T08:56:05.716022-08:00").unwrap(),
            /* 4 - binding for TIMESTAMP tested on #5 */
            /* 5 */
            DateTime::parse_from_rfc3339("2024-07-03T08:56:05.716022Z").unwrap(),
            /* 6 - binding on TIME tested on #7 */
            /* 7 */
            Value::Time64(TimeUnit::Second, 11 * 3600 + 30 * 60 + 10),
            /* 8 - binding on INTERVAL tested on #9 */
            /* 9 */
            Value::Interval { months: 7, days: 3, nanos: 72_101_202_000 }
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

        let _schema = format!("{:?}", batch.schema().field(8));
        let _debug = format!("{:?}", batch.column(8));

        // 0 - DATE (no binding)
        assert_eq!(
            batch.column(0).as_any().downcast_ref::<arrow_array::Date32Array>().unwrap().value(0),
            16375 /* # of days since 1970-01-01 */
        );

        // 1 - DATE (binding)
        assert_eq!(batch.column(1).as_any().downcast_ref::<arrow_array::Date32Array>().unwrap().value(0), 16375);

        // 2 - TIMESTAMP WITH TIME ZONE (no binding)
        assert_eq!(
            DateTime::<Utc>::decode(batch.column(2), 0),
            chrono::DateTime::parse_from_rfc3339("2024-07-03T15:56:05.716022Z").unwrap()
        );

        // 3 - TIMESTAMP WITH TIME ZONE (binding)
        assert_eq!(
            DateTime::<Utc>::decode(batch.column(3), 0),
            DateTime::parse_from_rfc3339("2024-07-03T08:56:05.716022-08:00").unwrap()
        );

        // 4 - TIMESTAMP (no binding)
        assert_eq!(
            DateTime::<Utc>::decode(batch.column(4), 0),
            DateTime::parse_from_rfc3339("2024-07-03T08:56:05.716022Z").unwrap()
        );

        // 5 - TIMESTAMP (binding)
        assert_eq!(
            DateTime::<Utc>::decode(batch.column(5), 0),
            DateTime::parse_from_rfc3339("2024-07-03T08:56:05.716022Z").unwrap()
        );

        // 6 - TIME (no binding)
        assert_eq!(
            NaiveTime::decode(batch.column(6), 0),
            NaiveTime::parse_from_str("11:30:00.123456", "%H:%M:%S%.f").unwrap()
        );

        // 7 - TIME (binding)
        assert_eq!(
            NaiveTime::decode(batch.column(7), 0),
            NaiveTime::parse_from_str("11:30:10", "%H:%M:%S%.f").unwrap()
        );

        /*
         * The following test is commended until next DuckDB release.
         * https://github.com/duckdb/duckdb-rs/issues/350

        // 8 - INTERVAL (no binding)
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(
            batch.column(8).as_any().downcast_ref::<arrow_array::IntervalMonthDayNanoArray>().unwrap().value(0),
        );
        assert_eq!(months, 12);
        assert_eq!(days, 5);
        assert_eq!(nanos, 12 * 60 * 60 * 1_000_000_000 + 13 * 1_000_000 + 8);

        // 9 - INTERVAL (binding)
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(
            batch.column(9).as_any().downcast_ref::<arrow_array::IntervalMonthDayNanoArray>().unwrap().value(0),
        );
        assert_eq!(months, 7);
        assert_eq!(days, 3);
        assert_eq!(nanos, 72_101_202_303);

        **/
    }

    #[test]
    fn test_binding_other_types() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        let mut stmt = conn
            .prepare(
                r#"SELECT 
              /* 0 */ '11011110101011011011111011101111'::BIT /* See https://github.com/duckdb/duckdb-rs/issues/349 */,
              /* 1 */ 12.99::DECIMAL(10, 2),
              /* 2 */ ?::DECIMAL(10, 2),
              /* 3 */ '5843cded-a32a-428d-8194-97ee1b949eb9'::UUID,
              /* 4 */ ?::UUID
              "#,
            )
            .unwrap();
        let batch = query_arrow!(
            stmt,
            /* 0 BIT (no binding) */
            /* 1 DECIMAL (no binding) */
            /* 2 */
            Value::Decimal(Decimal::new(2099, 2)),
            /* 3 UUID (no binding) */
            /* 4 */
            uuid::Uuid::parse_str("0e089c07-8654-4aab-9c25-4f3c44590251").unwrap()
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

        let _schema = format!("{:?}", batch.schema().field(1));
        let _debug = format!("{:?}", batch.column(1));

        // 0 - BIT
        // Not tested because because of a bug in DuckDB.
        // See https://github.com/duckdb/duckdb-rs/issues/349
        // assert_eq!(batch.column(15).as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap().value(0), vec![0xde, 0xad, 0xbe, 0xef]);

        // 1 - DECIMAL (no binding)
        assert_eq!(Decimal::decode(batch.column(1), 0), Decimal::from_i128_with_scale(1299, 2));

        // 2 - DECIMAL (binding)
        assert_eq!(Decimal::decode(batch.column(2), 0), Decimal::from_i128_with_scale(2099, 2));

        // 3 - UUID (no binding)
        assert_eq!(Uuid::decode(batch.column(3), 0), Uuid::parse_str("5843cded-a32a-428d-8194-97ee1b949eb9").unwrap());

        // 3 - UUID (binding)
        assert_eq!(Uuid::decode(batch.column(4), 0), Uuid::parse_str("0e089c07-8654-4aab-9c25-4f3c44590251").unwrap())
    }

    #[test]
    fn test_binding_null() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        let mut stmt = conn
            .prepare(
                r#"SELECT 
      /* 0 */ NULL,
      /* 1 */ ?,
      /* 2 */ ?
      "#,
            )
            .unwrap();
        let batch = query_arrow!(
            stmt,
            /* 0 NULL (no binding) */
            /* 1 */ Value::Null,
            /* 2 */ None::<i32>
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
        assert!(batch.column(2).is_null(0));
    }

    #[test]
    fn test_prepare_multiple_statements() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        let mut stmt_one = conn.prepare("SELECT 1").unwrap();
        let mut stmt_two = conn.prepare("SELECT 2").unwrap();

        let mut rows = query_arrow!(stmt_one).unwrap();
        assert_eq!(i32::decode(rows.next().unwrap().unwrap().column(0), 0), 1);
        drop(rows);

        let mut rows = query_arrow!(stmt_two).unwrap();
        assert_eq!(i32::decode(rows.next().unwrap().unwrap().column(0), 0), 2);

        let mut rows = query_arrow!(stmt_one).unwrap();
        assert_eq!(i32::decode(rows.next().unwrap().unwrap().column(0), 0), 1);
    }

    #[test]
    fn test_rebinding_statement() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        let mut stmt = conn.prepare("SELECT ?").unwrap();
        for i in 1..=2 {
            let mut rows = query_arrow!(stmt, i).unwrap();
            assert_eq!(i32::decode(rows.next().unwrap().unwrap().column(0), 0), i);
        }
    }
}
