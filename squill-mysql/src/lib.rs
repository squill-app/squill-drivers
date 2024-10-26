use squill_core::factory::Factory;

/// The name of the driver for MySQL.
pub const DRIVER_NAME: &str = "mysql";

mod driver;
mod errors;
mod factory;

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::MySqlFactory {}));
    });
}
