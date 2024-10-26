## Running tests

```sh
cargo test
```

The test suite is expecting to have an environment variable name `CI_MYSQL_URI`:

```sh
CI_MYSQL_URI=mysql://root:mypassword@localhost:3306/ci
```
