mod plugins;

use jbot_core::{init_log, jbot_main};

fn main() {
    dotenvy::dotenv().unwrap();

    let guard = init_log().unwrap();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed building the Runtime")
        .block_on(jbot_main());

    drop(guard);
}
