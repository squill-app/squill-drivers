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
    let rows: Rows = query_arrow!(stmt, 2)?.into();
    for next_row in rows {
        let row = next_row?;
        println!("id: {}, name: {}", row.column::<i64>(0), row.column::<String>(1));
    }

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
    let mut row = rows.next();
    if let Some(Ok(ref row)) = row.as_ref() {
        table.set_header(row.schema().fields().iter().map(|f| f.name().to_string()).collect::<Vec<String>>());
    }

    loop {
        match row {
            Some(Ok(row)) => {
                table.add_row(vec![row.column_by_name::<i64>("id").to_string(), row.column_by_name::<String>("name")]);
            }
            Some(Err(e)) => {
                return Err(e);
            }
            None => {
                break;
            }
        }
        row = rows.next();
    }
    println!("{table}");

    Ok(())
}
