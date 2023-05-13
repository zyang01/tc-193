use log::info;

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_micros().init();

    info!("Hello, world!");
}
