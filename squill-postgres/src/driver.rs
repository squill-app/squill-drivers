use crate::errors::into_driver_error;
use crate::values::ParametersIterator;
use crate::DRIVER_NAME;
use arrow_array::builder::ArrayBuilder;
use arrow_array::types::IntervalMonthDayNano;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use byteorder::{BigEndian, ReadBytesExt};
use postgres::fallible_iterator::FallibleIterator;
use postgres_types::{accepts, FromSql, Type};
use squill_core::arrow::array_builder::ArrayBuilderAppender;
use squill_core::driver::{DriverConnection, DriverOptionsRef, DriverStatement, Result};
use squill_core::parameters::Parameters;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct Postgres {
    pub(crate) client: postgres::Client,
    pub(crate) options: DriverOptionsRef,
}

impl DriverConnection for Postgres {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c: 's, 's>(&'c mut self, statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        Ok(Box::new(PostgresStatement {
            inner: self.client.prepare(statement).map_err(into_driver_error)?,
            client: &mut self.client,
            options: self.options.clone(),
        }))
    }
}

pub(crate) struct PostgresStatement<'c> {
    pub(crate) client: &'c mut postgres::Client,
    pub(crate) inner: postgres::Statement,
    pub(crate) options: DriverOptionsRef,
}

impl PostgresStatement<'_> {
    fn column_into_field(column: &postgres::Column) -> Field {
        let name = column.name().to_string();
        let data_type = match *column.type_() {
            postgres_types::Type::BOOL => DataType::Boolean,
            postgres_types::Type::CHAR => DataType::Int8,
            postgres_types::Type::INT2 => DataType::Int16,
            postgres_types::Type::INT4 => DataType::Int32,
            postgres_types::Type::INT8 => DataType::Int64,
            postgres_types::Type::FLOAT4 => DataType::Float32,
            postgres_types::Type::FLOAT8 => DataType::Float64,
            postgres_types::Type::DATE => DataType::Date32,
            postgres_types::Type::TIME => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
            postgres_types::Type::TIMESTAMP => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            postgres_types::Type::TIMESTAMPTZ => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            postgres_types::Type::INTERVAL => DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            postgres_types::Type::VARCHAR => DataType::Utf8,
            postgres_types::Type::JSON => DataType::Utf8,
            postgres_types::Type::JSONPATH => DataType::Utf8,
            postgres_types::Type::CSTRING => DataType::Utf8,
            postgres_types::Type::XML => DataType::Utf8,
            postgres_types::Type::CIDR => DataType::Utf8,
            postgres_types::Type::INET => DataType::Utf8,
            postgres_types::Type::TEXT => DataType::Utf8,
            postgres_types::Type::NAME => DataType::Utf8,
            postgres_types::Type::BPCHAR => DataType::Utf8,
            postgres_types::Type::UNKNOWN => DataType::Utf8,
            postgres_types::Type::OID => DataType::UInt32,
            postgres_types::Type::XID => DataType::UInt32,
            postgres_types::Type::CID => DataType::UInt32,
            // &postgres_types::Type::ARRAY => DataType::List(Box::new(Self::column_into_field(column.element_type().unwrap()))),
            _ => DataType::Binary,
        };

        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("datasource_type".to_string(), column.type_().to_string());
        Field::new(name, data_type, true).with_metadata(metadata)
    }
}

impl DriverStatement for PostgresStatement<'_> {
    fn execute(&mut self, _parameters: Option<Parameters>) -> Result<u64> {
        Ok(self.client.execute(&self.inner, &[]).map_err(into_driver_error)? as u64)
    }

    fn query<'s>(
        &'s mut self,
        parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        let params_iter = ParametersIterator::new(&parameters);
        let schema = self.schema();
        let res_iter = self.client.query_raw(&self.inner, params_iter).map_err(into_driver_error)?;
        let iter = PostgresRows { schema, inner: res_iter, options: self.options.clone() };
        Ok(Box::new(iter))
    }

    fn schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self.inner.columns().iter().map(Self::column_into_field).collect::<Vec<Field>>();
        Arc::new(Schema::new(fields))
    }
}

struct PostgresRows<'s> {
    schema: SchemaRef,
    options: DriverOptionsRef,
    inner: postgres::RowIter<'s>,
}

struct TextValue(String);

impl<'a> FromSql<'a> for TextValue {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> std::result::Result<TextValue, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            postgres_types::Type::CIDR => {
                let value = postgres_protocol::types::inet_from_sql(raw)?;
                Ok(TextValue(format!("{}/{}", value.addr(), value.netmask())))
            }
            postgres_types::Type::INET => {
                let value = postgres_protocol::types::inet_from_sql(raw)?;
                if value.netmask() == 32 {
                    Ok(TextValue(value.addr().to_string()))
                } else {
                    Ok(TextValue(format!("{}/{}", value.addr(), value.netmask())))
                }
            }
            _ => {
                let value = postgres_protocol::types::text_from_sql(raw)?;
                Ok(TextValue(value.into()))
            }
        }
    }
    accepts!(JSON, XML, JSONPATH, CSTRING, CIDR, INET);
}

struct BinaryValue(Vec<u8>);

impl<'a> FromSql<'a> for BinaryValue {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<BinaryValue, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            postgres_types::Type::MACADDR => {
                let mac_addr = postgres_protocol::types::macaddr_from_sql(raw)?;
                Ok(BinaryValue(mac_addr.to_vec()))
            }
            postgres_types::Type::UUID => {
                let mac_addr = postgres_protocol::types::uuid_from_sql(raw)?;
                Ok(BinaryValue(mac_addr.to_vec()))
            }
            _ => Ok(BinaryValue(raw.to_vec())),
        }
    }
    fn accepts(_: &Type) -> bool {
        true
    }
}

struct UInt32Value(u32);

impl<'a> FromSql<'a> for UInt32Value {
    fn from_sql(_: &Type, raw: &'a [u8]) -> std::result::Result<UInt32Value, Box<dyn std::error::Error + Sync + Send>> {
        let value = postgres_protocol::types::oid_from_sql(raw)?;
        Ok(UInt32Value(value))
    }
    accepts!(OID, XID, CID);
}

struct Int32Value(i32);

impl<'a> FromSql<'a> for Int32Value {
    fn from_sql(_: &Type, raw: &'a [u8]) -> std::result::Result<Int32Value, Box<dyn std::error::Error + Sync + Send>> {
        let value = postgres_protocol::types::int4_from_sql(raw)?;
        Ok(Int32Value(value))
    }
    accepts!(DATE);
}

struct Int64Value(i64);

impl<'a> FromSql<'a> for Int64Value {
    fn from_sql(_: &Type, raw: &'a [u8]) -> std::result::Result<Int64Value, Box<dyn std::error::Error + Sync + Send>> {
        let value = postgres_protocol::types::int8_from_sql(raw)?;
        Ok(Int64Value(value))
    }
    accepts!(MONEY, TIME, TIMESTAMP, TIMESTAMPTZ);
}

struct Interval {
    months: i32,
    days: i32,
    microseconds: i64,
}

impl<'a> FromSql<'a> for Interval {
    fn from_sql(_: &Type, raw: &'a [u8]) -> std::result::Result<Interval, Box<dyn std::error::Error + Sync + Send>> {
        let mut buf = raw;
        let microseconds: i64 = buf.read_i64::<BigEndian>()?;
        let days: i32 = buf.read_i32::<BigEndian>()?;
        let months: i32 = buf.read_i32::<BigEndian>()?;
        Ok(Interval { months, days, microseconds })
    }
    accepts!(INTERVAL);
}

impl PostgresRows<'_> {
    fn append_row(arrow_columns: &mut [Box<dyn ArrayBuilder>], row: postgres::Row) -> Result<()> {
        // https://www.npgsql.org/dev/types.html#overview
        for (index, row_column) in row.columns().iter().enumerate() {
            let builder = &mut arrow_columns[index];
            match *row_column.type_() {
                postgres_types::Type::BOOL => {
                    let value: Option<bool> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::CHAR => {
                    let value: Option<i8> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::INT2 => {
                    let value: Option<i16> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::INT4 => {
                    let value: Option<i32> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::OID | postgres_types::Type::XID | postgres_types::Type::CID => {
                    let value: Option<UInt32Value> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value.map(|v| v.0));
                }
                postgres_types::Type::INT8 => {
                    let value: Option<i64> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::FLOAT4 => {
                    let value: Option<f32> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::FLOAT8 => {
                    let value: Option<f64> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::VARCHAR
                | postgres_types::Type::TEXT
                | postgres_types::Type::NAME
                | postgres_types::Type::BPCHAR
                | postgres_types::Type::UNKNOWN => {
                    let value: Option<String> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::BYTEA => {
                    let value: Option<Vec<u8>> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::JSON
                | postgres_types::Type::XML
                | postgres_types::Type::CIDR
                | postgres_types::Type::INET
                | postgres_types::Type::JSONPATH
                | postgres_types::Type::CSTRING => {
                    let value: Option<TextValue> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value.map(|v| v.0));
                }
                postgres_types::Type::DATE => {
                    let value: Option<Int32Value> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value.map(|v| days_from_2000_to_unix(v.0)));
                }
                postgres_types::Type::TIMESTAMP | postgres_types::Type::TIMESTAMPTZ => {
                    let value: Option<Int64Value> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value.map(|v| microseconds_from_2000_to_unix(v.0)));
                }
                postgres_types::Type::TIME => {
                    let value: Option<Int64Value> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value.map(|v| v.0));
                }
                postgres_types::Type::INTERVAL => {
                    let value: Option<Interval> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value.map(|v| IntervalMonthDayNano {
                        months: v.months,
                        days: v.days,
                        nanoseconds: v.microseconds * 1_000,
                    }));
                }
                _ => {
                    let value: Option<BinaryValue> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value.map(|v| v.0));
                }
            }
        }
        Ok(())
    }
}

impl Iterator for PostgresRows<'_> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // `rows_affected`  will return `None` until the iterator has been exhausted
        if self.inner.rows_affected().is_some() {
            return None;
        }
        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|field| arrow_array::builder::make_builder(field.data_type(), 0))
            .collect::<Vec<_>>();

        let max_batch_rows = self.options.max_batch_rows;
        let mut row_num = 0;
        let inner = &mut self.inner;
        loop {
            let next_row = inner.next().map_err(into_driver_error);
            match next_row {
                Ok(Some(row)) => match Self::append_row(&mut columns, row) {
                    Ok(_) => {
                        row_num += 1;
                        if row_num >= max_batch_rows {
                            break;
                        }
                    }
                    Err(e) => return Some(Err(e)),
                },
                Ok(None) => break,
                Err(e) => return Some(Err(e.into())),
            };
        }
        match row_num {
            0 => None,
            _ => {
                let arrays: Vec<_> = columns.iter_mut().map(|builder| builder.finish()).collect();
                let batch = RecordBatch::try_new(self.schema.clone(), arrays);
                Some(batch.map_err(|e| e.into()))
            }
        }
    }
}

// Convert a PostgreSQL date to the number of days since UNIX epoch (1970-01-01).
#[inline]
fn days_from_2000_to_unix(days_from_2000: i32) -> i32 {
    const DAYS_FROM_2000_TO_UNIX: i32 = 10_957;
    days_from_2000 + DAYS_FROM_2000_TO_UNIX
}

// Convert a PostgreSQL timestamp to a number of microseconds since UNIX epoch (1970-01-01).
#[inline]
fn microseconds_from_2000_to_unix(microseconds_from_2000: i64) -> i64 {
    const MICROSECONDS_FROM_2000_TO_UNIX: i64 = 946_684_800_000_000;
    microseconds_from_2000 + MICROSECONDS_FROM_2000_TO_UNIX
}
