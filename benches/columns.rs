use criterion::{black_box, criterion_group, criterion_main, Criterion};
use squill_drivers::blocking_conn::{Connection, Statement};

fn using_get(stmt: &mut Statement) {
    let rows = stmt.query_rows(None).unwrap();
    for row in rows {
        let _: i32 = row.unwrap().get(0);
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    squill_core::mock::register_driver();
    let mut conn = Connection::open("mock://").unwrap();
    let mut stmt = conn.prepare("SELECT 1000").unwrap();

    let mut group = c.benchmark_group("Column vs Get Column");
    group.bench_function("get", |b| b.iter(|| using_get(black_box(&mut stmt))));
    group.finish();
}

// Criterion main function to run the benchmarks
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
