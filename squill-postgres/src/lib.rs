use squill_core::factory::Factory;

/// The name of the driver for PostgreSQL.
pub const DRIVER_NAME: &str = "postgres";

mod driver;
mod errors;
mod factory;
mod values;

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::PostgresFactory {}));
    });
}

#[cfg(test)]
mod postgres_tests {
    use arrow_array::ArrayRef;
    use chrono::NaiveDate;
    use ctor::ctor;
    use squill_core::assert_some;
    use squill_core::decode::Decode;
    use squill_core::driver::DriverConnection;
    use squill_core::{assert_execute_eq, assert_some_ok, factory::Factory};
    use tokio_test::assert_ok;
    use uuid::Uuid;

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    #[test]
    fn test_factory() {
        let ci_database_uri = env!("CI_POSTGRES_URI");
        assert_ok!(Factory::open(ci_database_uri));
    }

    #[test]
    fn test_ping() {
        let ci_database_uri = env!("CI_POSTGRES_URI");
        assert_ok!(assert_ok!(Factory::open(ci_database_uri)).ping());
    }

    #[test]
    fn test_execute() {
        let mut conn = assert_ok!(Factory::open(env!("CI_POSTGRES_URI")));
        assert_execute_eq!(conn, "CREATE TEMPORARY TABLE ci_test (id INTEGER PRIMARY KEY, name TEXT)", 0);
        assert_execute_eq!(conn, "INSERT INTO ci_test (id, name) VALUES (1, NULL)", 1);
        assert_execute_eq!(conn, "INSERT INTO ci_test (id, name) VALUES (2, NULL)", 1);
        assert_execute_eq!(conn, "DELETE FROM ci_test WHERE id IN (1, 2)", 2);
    }

    #[test]
    fn test_query() {
        let mut conn = assert_ok!(Factory::open(env!("CI_POSTGRES_URI")));
        let mut stmt = assert_ok!(conn.prepare("SELECT 1 AS col_one, 2 AS col_two"));
        let mut rows = assert_ok!(stmt.query(None));
        let record_batch = assert_some_ok!(rows.next());
        assert_eq!(record_batch.schema().fields().len(), 2);
        assert_eq!(record_batch.schema().field(0).name(), "col_one");
        assert_eq!(record_batch.schema().field(1).name(), "col_two");
        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(i32::decode(&record_batch.column(0), 0), 1);
        assert_eq!(i32::decode(&record_batch.column(1), 0), 2);
    }

    #[test]
    fn test_data_types() {
        fn get(conn: &mut Box<dyn DriverConnection>, expr: &str) -> ArrayRef {
            let sql = format!("SELECT {}", expr);
            let mut stmt = assert_ok!(conn.prepare(&sql));
            let mut rows = assert_ok!(stmt.query(None));
            assert_some_ok!(rows.next()).column(0).clone()
        }

        let mut conn = assert_ok!(Factory::open(env!("CI_POSTGRES_URI")));

        // INTERVAL - @ <number> <units>, time interval
        let interval = assert_some!(get(&mut conn, "'13 months 1 day 00:00:00.123456'::INTERVAL;")
            .as_any()
            .downcast_ref::<arrow_array::IntervalMonthDayNanoArray>())
        .value(0);
        assert_eq!(interval.months, 13);
        assert_eq!(interval.days, 1);
        assert_eq!(interval.nanoseconds, 123456000);

        // TIME - time of day
        assert_eq!(
            chrono::NaiveTime::decode(&get(&mut conn, "'20:18:51.577118'::TIME"), 0).to_string(),
            "20:18:51.577118"
        );

        // TIMESTAMPTZ - date and time
        assert_eq!(
            chrono::DateTime::<chrono::Utc>::decode(
                &get(&mut conn, "'2024-12-14 20:18:51.577118 EDT'::TIMESTAMPTZ"),
                0
            )
            .to_string(),
            "2024-12-15 00:18:51.577118 UTC"
        );

        // TIMESTAMP - date and time
        assert_eq!(
            chrono::DateTime::<chrono::Utc>::decode(&get(&mut conn, "'2024-12-14 20:18:51.577118'::TIMESTAMP"), 0)
                .to_string(),
            "2024-12-14 20:18:51.577118 UTC"
        );

        // BOOL - boolean, 'true'/'false'
        assert!(bool::decode(&get(&mut conn, "1::BOOL"), 0));

        // DATE - calendar date (year, month, day)
        assert_eq!(NaiveDate::decode(&get(&mut conn, "'2007-01-05'::DATE"), 0).to_string(), "2007-01-05");

        // NAME - 63-byte type for storing system identifiers
        assert_eq!(String::decode(&get(&mut conn, "'hello world'::NAME"), 0), "hello world");

        // INT2 - -32 thousand to 32 thousand, 2-byte storage
        assert_eq!(i16::decode(&get(&mut conn, "32767::INT2"), 0), i16::MAX);

        // INT4 - -2 billion to 2 billion integer, 4-byte storage
        assert_eq!(i32::decode(&get(&mut conn, "2147483647::INT4"), 0), i32::MAX);

        // INT8 - ~18 digit integer, 8-byte storage
        assert_eq!(i64::decode(&get(&mut conn, "9223372036854775807::INT8"), 0), i64::MAX);

        // FLOAT4 - single-precision floating point number, 4-byte storage
        assert_eq!(f32::decode(&get(&mut conn, "3.4028235e38::FLOAT4"), 0), f32::MAX);

        // FLOAT8 - double-precision floating point number, 8-byte storage
        assert_eq!(f64::decode(&get(&mut conn, "1.7976931348623157e308::FLOAT8"), 0), f64::MAX);

        // TEXT - variable-length string, no limit specified
        assert_eq!(String::decode(&get(&mut conn, "'hello world'::TEXT"), 0), "hello world");

        // OID - object identifier(oid), maximum 4 billion
        assert_eq!(u32::decode(&get(&mut conn, "4294967295::OID"), 0), u32::MAX);

        // XID - transaction id
        assert_eq!(u32::decode(&get(&mut conn, "'4294967295'::XID"), 0), u32::MAX);

        // CID - command identifier type, sequence in transaction id
        assert_eq!(u32::decode(&get(&mut conn, "'4294967295'::CID"), 0), u32::MAX);

        // JSON - JSON stored as text
        assert_eq!(String::decode(&get(&mut conn, r#"'{ "a": "hello" }'::JSON"#), 0), r#"{ "a": "hello" }"#);

        // MACADDR - XX:XX:XX:XX:XX:XX, MAC address
        assert_eq!(
            Vec::<u8>::decode(&get(&mut conn, "'e2:74:fe:ed:4b:d9'::MACADDR"), 0),
            vec![0xe2, 0x74, 0xfe, 0xed, 0x4b, 0xd9]
        );

        // MACADDR8 - XX:XX:XX:XX:XX:XX:XX:XX, MAC address
        assert_eq!(
            Vec::<u8>::decode(&get(&mut conn, "'08:00:2b:01:02:03:04:05'::MACADDR8"), 0),
            vec![0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]
        );

        // XML - XML content
        assert_eq!(String::decode(&get(&mut conn, "'<a>hello world</a>'::XML"), 0), "<a>hello world</a>");

        // JSONPATH - JSON path
        assert_eq!(String::decode(&get(&mut conn, "'$.address.city'::JSONPATH"), 0), "\u{1}$.\"address\".\"city\"");

        // CSTRING - C-style string
        assert_eq!(String::decode(&get(&mut conn, "'hello world'::CSTRING"), 0), "hello world");

        // UUID - UUID datatype
        assert_eq!(
            Uuid::decode(&get(&mut conn, "'e5143101-3ced-4a40-a77e-820a7654a2b0'::UUID"), 0),
            Uuid::parse_str("e5143101-3ced-4a40-a77e-820a7654a2b0").unwrap()
        );

        // CIDR - network IP address/netmask, network address
        assert_eq!(String::decode(&get(&mut conn, "'192.168.1.0/24'::CIDR"), 0), "192.168.1.0/24");

        // VARCHAR - varchar(length), non-blank-padded string, variable storage length
        assert_eq!(String::decode(&get(&mut conn, "'hello world'::VARCHAR(2)"), 0), "he");

        // BPCHAR - char(length), blank-padded string, fixed storage length
        assert_eq!(String::decode(&get(&mut conn, "'hello world'::BPCHAR(2)"), 0), "he");

        // INET - IP address/netmask, host address, netmask optional
        assert_eq!(String::decode(&get(&mut conn, "'192.168.1.0/24'::INET"), 0), "192.168.1.0/24");
        assert_eq!(String::decode(&get(&mut conn, "'192.168.1.0'::INET"), 0), "192.168.1.0");

        // JSONB - Binary JSON
        // JSONB binary is proprietary and not human-readable, to test we just check the length of the binary
        assert_eq!(Vec::<u8>::decode(&get(&mut conn, r#"'{ "a": "hello" }'::JSONB"#), 0).len(), 15);

        // UNKNOWN - pseudo-type representing an undetermined type
        assert_eq!(String::decode(&get(&mut conn, "'hello'::UNKNOWN"), 0), "hello");

        // NUMERIC - numeric(precision, decimal), arbitrary precision number
        // assert_eq!(String::decode(&get(&mut conn, "123.991::NUMERIC(10, 2)"), 0), "hello");

        // BYTEA - variable-length string, binary values escaped
        // CHAR - single character
        // INT2VECTOR - array of int2, used in system tables
        // REGPROC - registered procedure
        // TID - (block, offset), physical location of tuple
        // OIDVECTOR - array of oids, used in system tables
        // PG_DDL_COMMAND - internal type for passing CollectedCommand
        // XML[]
        // PG_NODE_TREE - string representing an internal node tree
        // JSON[]
        // TABLE_AM_HANDLER
        // XID8[]
        // INDEX_AM_HANDLER - pseudo-type for the result of an index AM handler function
        // POINT - geometric point '(x, y)'
        // LSEG - geometric line segment '(pt1,pt2)'
        // PATH - geometric path '(pt1,...)'
        // BOX - geometric box '(lower left,upper right)'
        // POLYGON - geometric polygon '(pt1,...)'
        // LINE - geometric line
        // LINE[]
        // CIDR[]
        // CIRCLE - geometric circle '(center,radius)'
        // CIRCLE[]
        // MACADDR8[]
        // MONEY - monetary amounts, $d,ddd.cc
        // MONEY[]
        // BYTEA[]
        // CHAR[]
        // INT2VECTOR[]
        // INT4[]
        // REGPROC[]
        // TEXT[]
        // TID[]
        // XID[]
        // CID[]
        // OIDVECTOR[]
        // BPCHAR[]
        // VARCHAR[]
        // INT8[]
        // POINT[]
        // LSEG[]
        // PATH[]
        // BOX[]
        // FLOAT4[]
        // FLOAT8[]
        // POLYGON[]
        // OID[]
        // ACLITEM - access control list
        // ACLITEM[]
        // MACADDR[]
        // INET[]
        // TIMESTAMP[]
        // DATE[]
        // TIME[]
        // TIMESTAMPTZ[]
        // INTERVAL[]
        // NUMERIC[]
        // CSTRING[]
        // TIMETZ - time of day with time zone
        // TIMETZ[]
        // BIT - fixed-length bit string
        // BIT[]
        // VARBIT - variable-length bit string
        // VARBIT[]
        // REFCURSOR - reference to cursor (portal name)
        // REFCURSOR[]
        // REGPROCEDURE - registered procedure (with args)
        // REGOPER - registered operator
        // REGOPERATOR - registered operator (with args)
        // REGCLASS - registered class
        // REGTYPE - registered type
        // REGPROCEDURE[]
        // REGOPER[]
        // REGOPERATOR[]
        // REGCLASS[]
        // REGTYPE[]
        // RECORD - pseudo-type representing any composite type
        // ANY - pseudo-type representing any type
        // ANYARRAY - pseudo-type representing a polymorphic array type
        // VOID - pseudo-type for the result of a function with no real result
        // TRIGGER - pseudo-type for the result of a trigger function
        // LANGUAGE_HANDLER - pseudo-type for the result of a language handler function
        // INTERNAL - pseudo-type representing an internal data structure
        // ANYELEMENT - pseudo-type representing a polymorphic base type
        // RECORD[]
        // ANYNONARRAY - pseudo-type representing a polymorphic base type that is not an array
        // TXID_SNAPSHOT[]
        // UUID[]
        // TXID_SNAPSHOT - txid snapshot
        // FDW_HANDLER - pseudo-type for the result of an FDW handler function
        // PG_LSN - PostgreSQL LSN datatype
        // PG_LSN[]
        // TSM_HANDLER - pseudo-type for the result of a tablesample method function
        // PG_NDISTINCT - multivariate ndistinct coefficients
        // PG_DEPENDENCIES - multivariate dependencies
        // ANYENUM - pseudo-type representing a polymorphic base type that is an enum
        // TSVECTOR - text representation for text search
        // TSQUERY - query representation for text search
        // GTSVECTOR - GiST index internal text representation for text search
        // TSVECTOR[]
        // GTSVECTOR[]
        // TSQUERY[]
        // REGCONFIG - registered text search configuration
        // REGCONFIG[]
        // REGDICTIONARY - registered text search dictionary
        // REGDICTIONARY[]
        // JSONB[]
        // ANYRANGE - pseudo-type representing a range over a polymorphic base type
        // EVENT_TRIGGER - pseudo-type for the result of an event trigger function
        // INT4RANGE - range of integers
        // INT4RANGE[]
        // NUMRANGE - range of numerics
        // NUMRANGE[]
        // TSRANGE - range of timestamps without time zone
        // TSRANGE[]
        // TSTZRANGE - range of timestamps with time zone
        // TSTZRANGE[]
        // DATERANGE - range of dates
        // DATERANGE[]
        // INT8RANGE - range of bigints
        // INT8RANGE[]
        // JSONPATH[]
        // REGNAMESPACE - registered namespace
        // REGNAMESPACE[]
        // REGROLE - registered role
        // REGROLE[]
        // REGCOLLATION - registered collation
        // REGCOLLATION[]
        // INT4MULTIRANGE - multirange of integers
        // NUMMULTIRANGE - multirange of numerics
        // TSMULTIRANGE - multirange of timestamps without time zone
        // TSTZMULTIRANGE - multirange of timestamps with time zone
        // DATEMULTIRANGE - multirange of dates
        // INT8MULTIRANGE - multirange of bigints
        // ANYMULTIRANGE - pseudo-type representing a polymorphic base type that is a multirange
        // ANYCOMPATIBLEMULTIRANGE - pseudo-type representing a multirange over a polymorphic common type
        // PG_BRIN_BLOOM_SUMMARY - BRIN bloom summary
        // PG_BRIN_MINMAX_MULTI_SUMMARY - BRIN minmax-multi summary
        // PG_MCV_LIST - multivariate MCV list
        // PG_SNAPSHOT - snapshot
        // PG_SNAPSHOT[]
        // XID8 - full transaction id
        // ANYCOMPATIBLE - pseudo-type representing a polymorphic common type
        // ANYCOMPATIBLEARRAY - pseudo-type representing an array of polymorphic common type elements
        // ANYCOMPATIBLENONARRAY - pseudo-type representing a polymorphic common type that is not an array
        // ANYCOMPATIBLERANGE - pseudo-type representing a range over a polymorphic common type
        // INT4MULTIRANGE[]
        // NUMMULTIRANGE[]
        // TSMULTIRANGE[]
        // TSTZMULTIRANGE[]
        // DATEMULTIRANGE[]
        // INT8MULTIRANGE[]
        // JSONPATH[]
    }
}
