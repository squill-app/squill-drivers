> A simple set of drivers for accessing various database systems.

[![build](https://img.shields.io/github/actions/workflow/status/squill-app/squill-drivers/build.yml?style=for-the-badge)](https://github.com/squill-app/squill-drivers/actions/workflows/build.yml)
[![codecov](https://img.shields.io/codecov/c/gh/squill-app/squill-drivers/settings/badge.svg?token=MZQU5H90OW&style=for-the-badge&logo=codecov)](https://codecov.io/github/squill-app/squill-drivers)

The aim of this crate is to provide a unified interface for various database systems. Each database system is accessed
through a minimalist low-level blocking trait called **driver** and high-level blocking and non-blocking interfaces
built on top of that trait are exposed to the developers.

In a nutshell, here are the main features provided by this crate:

- **Connections are created via a URI:**

  ```rust
  let conn = Connection::open("duckdb://:memory:")?;
  ```

  Once a connection opened, it can be used to access the database with the same interface regardless of the database
  system.

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

## Special Thanks

A special thanks to [Quine Dot](https://users.rust-lang.org/u/quinedot/summary) who has been a tremendous help for me
on the [RUST lang user's forum](https://users.rust-lang.org)...
