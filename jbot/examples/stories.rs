use std::sync::Arc;

use grammers_client::Client;
// use grammers_client::message::InputMessage;
use grammers_client::sender::SenderPool;
use grammers_session::storages::SqliteSession;
use grammers_session::types::PeerId;
use grammers_tl_types as tl;
use tracing::info;

const SESSION_FILE: &str = "hbcaomiao.session";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tracing::Level;
    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = tracing_subscriber::filter::Targets::new().with_default(Level::INFO);

    let layer = tracing_subscriber::fmt::layer()
        .without_time()
        .with_file(false)
        .map_fmt_fields(|f| {
            use tracing_subscriber::field::MakeExt;
            f.debug_alt()
        });
    tracing_subscriber::registry()
        .with(layer)
        .with(filter)
        .init();

    let api_id = 11773602;
    let session = Arc::new(SqliteSession::open(SESSION_FILE).await.unwrap());

    let SenderPool { runner, handle, .. } = SenderPool::new(Arc::clone(&session), api_id);
    let client = Client::new(handle.clone());
    let pool_task = tokio::spawn(runner.run());

    if !client.is_authorized().await.unwrap() {
        return Err("Unauthorized".into());
    }

    // let peer_id = PeerId::self_user();
    // let peer_ref = peer_id.to_ambient_ref();

    let res = client
        .invoke(&tl::functions::stories::GetPinnedStories {
            peer: tl::types::InputPeerUser {
                user_id: 1953423354,
                access_hash: 4409572011563736578,
            }
            .into(),
            offset_id: 0,
            limit: 10,
        })
        .await?;
    dbg!(res);

    info!("Gracefully closing connection to notify all pending handlers...");
    handle.quit();
    let _ = pool_task.await;

    Ok(())
}
