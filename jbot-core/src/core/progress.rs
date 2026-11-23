use std::fmt::Write;
use std::iter::repeat;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use grammers_client::client::Client;
use grammers_client::message::{InputMessage, Message};
use tokio::io::{self, AsyncRead, ReadBuf};
use tokio::sync::mpsc;
use tracing::error;

const UPDATE_LIMIT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProgressStyle {
    /// {p} / {total}
    Plain,
    /// 百分比，如 `45.67%`
    #[default]
    Percent,
    /// 文件大小，如 `1.23 MB / 10.00 MB`
    Size,
    /// 时间，如 `05:00 / 01:05:45`
    Time,
}

enum Command {
    Update { p: usize, total: Option<usize> },
    Add(usize),
    SetTotal(usize),
    SetStyle(ProgressStyle),
    SetPrefix(String),
    Quit,
    Delete,
    Edit(InputMessage),
    Rebuild { client: Client },
}

struct Rebuilding {
    client: Client,
}

pub struct Progress {
    pub handle: ProgressHandle,
    pub runner: ProgressRunner,
}

#[derive(Clone)]
pub struct ProgressHandle(mpsc::UnboundedSender<Command>);

pub struct ProgressRunner {
    mid: Message,
    rx: mpsc::UnboundedReceiver<Command>,
    deleted: bool,
    rebuilding: Option<Rebuilding>,

    p: usize,
    total: usize,
    prefix: String,
    style: ProgressStyle,
    last_update: Instant,
    last_text: Option<String>,
    message_id_invalid: bool,
}

impl Progress {
    pub fn new(mid: Message) -> Self {
        let now = Instant::now();
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            handle: ProgressHandle(tx),
            runner: ProgressRunner {
                mid,
                rx,
                deleted: false,
                rebuilding: None,
                p: 0,
                total: 100,
                prefix: String::new(),
                style: ProgressStyle::Percent,
                last_update: now.checked_sub(UPDATE_LIMIT).unwrap_or(now),
                last_text: None,
                message_id_invalid: false,
            },
        }
    }

    pub fn total(mut self, total: usize) -> Self {
        self.runner.total = total;
        self
    }

    pub fn prefix(mut self, prefix: &str) -> Self {
        self.runner.prefix = prefix.to_string();
        self
    }

    pub fn style(mut self, style: ProgressStyle) -> Self {
        self.runner.style = style;
        self
    }
}

impl ProgressHandle {
    pub fn update(&self, p: usize, total: Option<usize>) -> bool {
        self.0.send(Command::Update { p, total }).is_ok()
    }

    pub fn add(&self, add: usize) -> bool {
        self.0.send(Command::Add(add)).is_ok()
    }

    pub fn set_total(&self, total: usize) -> bool {
        self.0.send(Command::SetTotal(total)).is_ok()
    }

    pub fn set_prefix(&self, prefix: &str) -> bool {
        self.0.send(Command::SetPrefix(prefix.to_string())).is_ok()
    }

    pub fn set_style(&self, style: ProgressStyle) -> bool {
        self.0.send(Command::SetStyle(style)).is_ok()
    }

    pub fn finish(&self) -> bool {
        self.0.send(Command::Quit).is_ok()
    }

    pub fn delete(&self) -> bool {
        self.0.send(Command::Delete).is_ok()
    }

    pub fn edit<M: Into<InputMessage>>(&self, message: M) -> bool {
        self.0.send(Command::Edit(message.into())).is_ok()
    }

    pub fn rebuild(&self, client: Client) -> bool {
        self.0.send(Command::Rebuild { client }).is_ok()
    }
}

impl ProgressRunner {
    const CHARS: [char; 9] = [
        '\u{3000}', // 全角空格
        '\u{258f}', // 1/8 方块
        '\u{258e}', // 2/8 方块
        '\u{258d}', // 3/8 方块
        '\u{258c}', // 4/8 方块
        '\u{258b}', // 5/8 方块
        '\u{258a}', // 6/8 方块
        '\u{2589}', // 7/8 方块
        '\u{2588}', // 全方块
    ];

    pub async fn run(mut self) -> Message {
        while let Some(command) = self.rx.recv().await {
            let flow = self.process_command(command).await;
            match flow {
                ControlFlow::Continue(()) => continue,
                ControlFlow::Break(()) => break,
            }
        }
        self.mid
    }

    async fn process_command(&mut self, command: Command) -> ControlFlow<()> {
        match command {
            Command::Update { p, total } => {
                self.update(p, total).await;
                ControlFlow::Continue(())
            }
            Command::Add(add) => {
                self.add(add).await;
                ControlFlow::Continue(())
            }
            Command::SetTotal(total) => {
                self.total = total;
                ControlFlow::Continue(())
            }
            Command::SetStyle(style) => {
                self.style = style;
                ControlFlow::Continue(())
            }
            Command::SetPrefix(prefix) => {
                self.prefix = prefix;
                ControlFlow::Continue(())
            }
            Command::Quit => ControlFlow::Break(()),
            Command::Delete => {
                self.delete().await;
                ControlFlow::Break(())
            }
            Command::Edit(message) => {
                self.edit(message).await;
                ControlFlow::Continue(())
            }
            Command::Rebuild { client } => {
                self.rebuilding = Some(Rebuilding { client });
                self.deleted = false;
                self.delete().await;
                ControlFlow::Continue(())
            }
        }
    }

    fn format_size(bytes: usize) -> String {
        const KB: f64 = 1024.0;
        const MB: f64 = KB * 1024.0;
        const GB: f64 = MB * 1024.0;

        let n = bytes as f64;
        if n >= GB {
            format!("{:.2} GB", n / GB)
        } else if n >= MB {
            format!("{:.2} MB", n / MB)
        } else if n >= KB {
            format!("{:.2} KB", n / KB)
        } else {
            format!("{bytes} B")
        }
    }

    fn format_time(secs: usize) -> String {
        let hours = secs / 3600;
        let minutes = (secs % 3600) / 60;
        let seconds = secs % 60;

        if hours > 0 {
            format!("{hours:02}:{minutes:02}:{seconds:02}")
        } else {
            format!("{minutes:02}:{seconds:02}")
        }
    }

    fn get_text(&self) -> String {
        const CELLS: usize = 13;
        const STEPS: usize = CELLS * 8;

        let total = self.total;
        let p = self.p.min(total);
        let x = if total == 0 {
            0
        } else {
            (p as u64 * STEPS as u64 / total as u64) as usize
        };
        let (full, part) = (x / 8, x % 8);

        let mut text = String::with_capacity(self.prefix.len() + CELLS * 3 + 48);
        text.push_str(&self.prefix);
        text.push('[');
        text.extend(repeat(Self::CHARS[8]).take(full));
        if x != STEPS {
            text.push(Self::CHARS[part]);
            text.extend(repeat(Self::CHARS[0]).take(CELLS - full - 1));
        }
        text.push(']');
        text.push(' ');
        match self.style {
            ProgressStyle::Plain => write!(text, "{} / {}", p, total).unwrap(),
            ProgressStyle::Percent => {
                if total == 0 {
                    text.push_str("0.00%");
                } else {
                    write!(text, "{:.2}%", (p as f64 / total as f64) * 100.0).unwrap();
                }
            }
            ProgressStyle::Size => {
                write!(
                    text,
                    "{} / {}",
                    Self::format_size(p),
                    Self::format_size(total)
                )
                .unwrap();
            }
            ProgressStyle::Time => {
                write!(
                    text,
                    "{} / {}",
                    Self::format_time(p),
                    Self::format_time(total)
                )
                .unwrap();
            }
        }

        text
    }

    async fn update(&mut self, p: usize, total: Option<usize>) {
        self.p = p;
        if let Some(t) = total {
            self.total = t;
        }

        self.update_display().await;
    }

    async fn add(&mut self, add: usize) {
        self.p = self.p.saturating_add(add);

        self.update_display().await;
    }

    async fn update_display(&mut self) {
        if self.message_id_invalid {
            return;
        }
        if self.last_update.elapsed() < UPDATE_LIMIT {
            return;
        }

        let text = self.get_text();
        if Some(&text) == self.last_text.as_ref() {
            return;
        }

        if self.edit(InputMessage::new().text(&text)).await {
            self.last_update = Instant::now();
            self.last_text = Some(text);
        }
    }

    async fn edit(&mut self, message: InputMessage) -> bool {
        if let Some(Rebuilding { client }) = self.rebuilding.take() {
            let Ok(Some(peer_ref)) = self.mid.peer_ref().await else {
                return false;
            };
            let reply_to = self.mid.reply_to_message_id();
            match client
                .send_message(peer_ref, message.reply_to(reply_to))
                .await
            {
                Ok(message) => {
                    self.mid = message;
                    true
                }
                Err(e) => {
                    error!("进度条重发失败: {e}");
                    if e.is("MESSAGE_ID_INVALID") {
                        self.message_id_invalid = true;
                    }
                    false
                }
            }
        } else {
            match self.mid.edit(message).await {
                Ok(_) => true,
                Err(e) => {
                    error!("进度条更新失败: {e}");
                    if e.is("MESSAGE_ID_INVALID") {
                        self.message_id_invalid = true;
                    }
                    false
                }
            }
        }
    }

    async fn delete(&mut self) -> bool {
        if self.deleted {
            return true;
        }
        self.deleted = true;
        match self.mid.delete().await {
            Ok(_) => true,
            Err(e) => {
                error!("删除进度条失败: {e}");
                false
            }
        }
    }
}

pub struct ProgressReader<T, F> {
    inner: T,
    on_progress: F,
    read: usize,
}

impl<T, F> ProgressReader<T, F> {
    pub fn new(inner: T, on_progress: F) -> Self
    where
        T: AsyncRead + Unpin,
        F: FnMut(usize) + Unpin,
    {
        Self {
            inner,
            on_progress,
            read: 0,
        }
    }
}

impl<T, F> AsyncRead for ProgressReader<T, F>
where
    T: AsyncRead + Unpin,
    F: FnMut(usize) + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();

        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        };

        let n = buf.filled().len() - before;
        if n > 0 {
            this.read += n;
            (this.on_progress)(this.read);
        }

        Poll::Ready(Ok(()))
    }
}
