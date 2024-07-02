use squill_drivers::{ Connection, Rows, Result, query };
use comfy_table::Table;

fn main() -> Result<()> {
    squill_drivers::register_drivers();

    let conn = Connection::open("duckdb:///:memory:")?;

    //
    // Displaying the results of a query using the Rows iterator
    //
    //   id: 1, name: Employee #1
    //   id: 2, name: Employee #2
    //   id: 3, name: Employee #3
    //
    let rows: Rows = query!(
        conn,
        r#"SELECT id, 'Employee #' || id as name
             FROM generate_series(1, 3) AS series(id)"#
    )?.into();
    for next_row in rows {
        let row = next_row?;
        println!("id: {}, name: {}", row.column::<i64>(0), row.column::<String>(1));
    }

    //
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
    let mut rows: Rows = query!(
        conn,
        r#"SELECT id, 'Employee #' || id as name
             FROM generate_series(1, 3) AS series(id)"#
    )?.into();

    let mut table = Table::new();
    table.load_preset(comfy_table::presets::UTF8_HORIZONTAL_ONLY);
    let mut row = rows.next();
    if let Some(Ok(ref row)) = row.as_ref() {
        table.set_header(
            row
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<String>>()
        );
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
