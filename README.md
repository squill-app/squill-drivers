> A simple set of drivers for accessing various database systems.

[![build](https://img.shields.io/github/actions/workflow/status/squill-app/squill-drivers/check.yml?style=for-the-badge)](https://github.com/squill-app/squill-drivers/actions/workflows/check.yml)
[![codecov](https://img.shields.io/codecov/c/gh/squill-app/squill-drivers/settings/badge.svg?token=MZQU5H90OW&style=for-the-badge&logo=codecov)](https://codecov.io/github/squill-app/squill-drivers)

The aim of this crate is to provide a unified interface for various database systems. Each database system is accessed
through a simple synchronous interface called `driver` but this crate also provides an asynchronous interface built on
top of the those drivers.

In a nutshell, here are the main features provided by this crate:

- **Connections are created via a factory:**

  ```rust
  let conn = Connection::open("duckdb://:memory:")?;
  ```

  The factory returns a `Box<dyn DriverConnection>` which then can be used to access the database with the same
  interface regardless of the database system.

- **Execution via `execute` method for statement that don't return data:**

  ```rust
  let affected_rows = execute!(conn, "INSERT INTO employees (id, name) VALUES (1, 'McFly')");
  ```

- **Parameters bindings:**

  ```rust
  let affected_rows = execute!(conn, "INSERT INTO employees (id, name) VALUES (?, ?)", 1, "Mc Fly");
  ```

- **Execution via `query` to fetch data:**

  ```rust

  ```

  By default data are returned using the [Arrow](https://arrow.apache.org) format using
  [RecordBatch](https://docs.rs/arrow-array/52.0.0/arrow_array/struct.RecordBatch.html) but an adapter is provided to
  handle a query result as rows for convenience:

  ```rust
  let rows: Rows = query!(conn, "SELECT id, name FROM employee");
  for (row_result in rows) {
      let row = row_result?;
      println!(format("id: {}, name: {}", row.column::<i64>(0), row.column::<String>(1)));
  }
  ```

- **Support of asynchronous operations**:

  ```

  ```

## Why using Arrow?

The short answer is _why reinventing the wheel?_. Because `squill` drivers are intended to interface with a wide range
of database systems, each having their own data types and description a of a query result, we need an independent memory
format and an independent schema to represent data in memory and describe them.
[Arrow](https://arrow.apache.org/docs/python/api/datatypes.html#data-types-and-schemas) provide both and more...
