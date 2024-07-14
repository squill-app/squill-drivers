use comfy_table::Table;
use squill_core::query_arrow;
use squill_drivers::{Connection, Result, Rows};

fn main() -> Result<()> {
    squill_drivers::register_drivers();

    let conn = Connection::open("duckdb:///:memory:")?;

    // Preparing a query statement that takes 1 parameter
    let mut stmt = conn.prepare("SELECT id, 'Employee #' || id as name FROM generate_series(1, ?) AS series(id)")?;

    // Displaying the results of a query using the Rows iterator
    //
    //   id: 1, name: Employee #1
    //   id: 2, name: Employee #2
    //
    let mut rows: Rows = query_arrow!(stmt, 2)?.into();
    while let Some(Ok(row)) = rows.next() {
        let employee: (i64, String) = (row.get("id"), row.get("name"));
        println!("id: {}, name: {}", employee.0, employee.1);
    }
    drop(rows); // because rows borrows the statement, we need to drop it before reusing the statement

    println!();

    // Same as above, but displaying the results in a table and accessing the columns values by name
    //
    //   ──────────────────
    //   id   name
    //   ══════════════════
    //   1    Employee #1
    //   ──────────────────
    //   2    Employee #2
    //   ──────────────────
    //   3    Employee #3
    //   ──────────────────
    let mut rows: Rows = query_arrow!(stmt, 3)?.into();
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::UTF8_HORIZONTAL_ONLY);
    let mut next_row = rows.next();
    if let Some(Ok(ref first_row)) = next_row.as_ref() {
        table.set_header(first_row.schema().fields().iter().map(|f| f.name().to_string()).collect::<Vec<String>>());
        while let Some(Ok(row)) = next_row {
            let employee: (i64, String) = (row.get("id"), row.get("name"));
            table.add_row(vec![employee.0.to_string(), employee.1.to_string()]);
            next_row = rows.next();
        }
        println!("{table}");
    }

    Ok(())
}
