use std::sync::Arc;

use grammers_client::Client;
use grammers_client::message::InputMessage;
use grammers_client::sender::SenderPool;
use grammers_session::storages::SqliteSession;
use grammers_session::types::PeerId;
use grammers_tl_types as tl;
use tracing::info;

const SESSION_FILE: &str = "hbcao4bot.session";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api_id = 11773602;
    let api_hash = "9e72321cb6b97f7cd022f8c7898780ed";
    let token = "7704900702:AAEFK0x44R7PQA5ZSnbBIg1x4cswcx2yri0";

    let session = Arc::new(SqliteSession::open(SESSION_FILE).await.unwrap());

    let SenderPool { runner, handle, .. } = SenderPool::new(Arc::clone(&session), api_id);
    let client = Client::new(handle.clone());
    let pool_task = tokio::spawn(runner.run());

    if !client.is_authorized().await.unwrap() {
        info!("Signing in...");
        client
            .bot_sign_in(&token, &api_hash)
            .await
            .expect("Sign in failed.");
        info!("Signed in!");
    }

    let peer_id = PeerId::user(8194173941).unwrap();
    let peer_ref = peer_id.to_ambient_ref();
    let peer_ref = client
        .resolve_peer(peer_ref)
        .await?
        .to_ref()
        .await?
        .unwrap_or(peer_ref);

    let reply_to = None; //Some(212116);
    let thumb = client.upload_file("target/1.jpg").await?.raw;
    let msg = "<a href=\"https://www.youtube.com/watch?v=5soO064de4w\">自由花—Flower of Freedom</a> - <a href=\"https://www.youtube.com/channel/UCDIXSZ0hC5s6WLAtI0MdE0A\">Chiyomi Anchovy 安丘比</a> #YouTuBe
<blockquote expandable>1989~2023

本影片中所用的圖片，音頻與影片中，版權屬於原作者，並不屬於我。本頻道的影片只在於分享和記錄歷史，沒有對任何國家，組織與個人進行攻擊。
The images, audio, and visuals used in this video belong to their respective owners. The content of this channel is for the sole purpose of sharing and recording history; it is not intended to target any individuals, groups or countries.</blockquote>";
    let name = String::from("youtube_5soO064de4w.mp4");
    let path = "target/youtube_5soO064de4w.mp4";
    let uploaded = client.upload_file(path).await?;
    let media = tl::types::InputMediaUploadedDocument {
        nosound_video: true,
        force_file: false,
        spoiler: false,
        file: uploaded.raw,
        thumb: Some(thumb),
        mime_type: "video/mp4".to_string(),
        attributes: vec![
            tl::types::DocumentAttributeFilename { file_name: name }.into(),
            tl::types::DocumentAttributeVideo {
                round_message: false,
                supports_streaming: true,
                nosound: false,
                duration: 253.515465,
                w: 1920,
                h: 1080,
                preload_prefix_size: None,
                video_start_ts: None,
                video_codec: None,
            }
            .into(),
        ],
        stickers: None,
        ttl_seconds: None,
        video_cover: None,
        video_timestamp: None,
    };
    client
        .send_message(
            peer_ref,
            InputMessage::new()
                .html(msg)
                .media(media)
                .reply_to(reply_to),
        )
        .await?;

    info!("Gracefully closing connection to notify all pending handlers...");
    handle.quit();
    let _ = pool_task.await;

    Ok(())
}
