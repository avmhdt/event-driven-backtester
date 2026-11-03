//! Blocking WebSocket connections over rustls (ring provider) or, for the
//! loopback mock venue only, plain TCP. Every URL passes the host guard.

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rustls::pki_types::ServerName;
use tungstenite::{Message, WebSocket};

use crate::LiveError;
use crate::guard::HostGuard;
use crate::wall_clock::wall_ns;

/// TCP or TLS stream under a WebSocket.
#[derive(Debug)]
pub enum WsStream {
    /// Plain TCP (loopback mock venue).
    Plain(TcpStream),
    /// TLS.
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl WsStream {
    fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(s) => s,
            Self::Tls(s) => &s.sock,
        }
    }
}

impl Read for WsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for WsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

/// A connected socket.
pub type Ws = WebSocket<WsStream>;

fn tls_config() -> Result<Arc<rustls::ClientConfig>, LiveError> {
    let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| LiveError::Http(format!("tls: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// Connect to `url` (guard-checked) and complete the WebSocket handshake.
/// Reads time out after `read_timeout` so callers can poll stop flags.
///
/// # Errors
/// Guard, DNS, TCP, TLS or handshake errors.
pub fn connect(url: &str, guard: &HostGuard, read_timeout: Duration) -> Result<Ws, LiveError> {
    let parsed = guard.check_url(url)?;
    let host = parsed.host_str().ok_or_else(|| LiveError::Http(format!("no host in {url}")))?.to_owned();
    let tls = parsed.scheme() == "wss";
    let port = parsed.port_or_known_default().unwrap_or(if tls { 443 } else { 80 });
    let bare = host.trim_start_matches('[').trim_end_matches(']').to_owned();
    let addr = (bare.as_str(), port)
        .to_socket_addrs()
        .map_err(|e| LiveError::Http(format!("resolve {host}: {e}")))?
        .next()
        .ok_or_else(|| LiveError::Http(format!("no address for {host}")))?;
    let sock = TcpStream::connect_timeout(&addr, Duration::from_secs(10))
        .map_err(|e| LiveError::Http(format!("connect {host}: {e}")))?;
    sock.set_nodelay(true).map_err(|e| LiveError::Http(e.to_string()))?;
    sock.set_read_timeout(Some(Duration::from_secs(10))).map_err(|e| LiveError::Http(e.to_string()))?;
    let stream = if tls {
        let name = ServerName::try_from(bare).map_err(|e| LiveError::Http(format!("tls name: {e}")))?;
        let conn =
            rustls::ClientConnection::new(tls_config()?, name).map_err(|e| LiveError::Http(format!("tls: {e}")))?;
        WsStream::Tls(Box::new(rustls::StreamOwned::new(conn, sock)))
    } else {
        WsStream::Plain(sock)
    };
    let (ws, _resp) = tungstenite::client::client(url, stream)
        .map_err(|e| LiveError::Http(format!("websocket handshake {host}: {}", super::rest::redact(&e.to_string()))))?;
    ws.get_ref().tcp().set_read_timeout(Some(read_timeout)).map_err(|e| LiveError::Http(e.to_string()))?;
    Ok(ws)
}

/// Why a read loop ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ended {
    /// The stop flag was set.
    Stopped,
    /// The connection reached its maximum age (proactive reconnect).
    Rotate,
    /// No message for longer than the stale limit.
    Stale,
    /// The peer closed the connection.
    Closed,
    /// A read error.
    Error(String),
    /// The handler asked to end the loop.
    Handler(String),
}

impl Ended {
    /// Short reason code for gap and drop logs.
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::Stopped => "stop",
            Self::Rotate => "rotate",
            Self::Stale => "stale",
            Self::Closed => "closed",
            Self::Error(_) => "error",
            Self::Handler(r) => r,
        }
    }
}

/// Read messages until stopped, closed, stale or older than `max_age`.
/// `on_text(ts_recv, text)` returns `Err(reason)` to end the loop; `poll`
/// runs about once per read timeout (keepalives).
pub fn read_loop(
    ws: &mut Ws,
    stop: &AtomicBool,
    max_age: Duration,
    stale_after: Duration,
    mut on_text: impl FnMut(i64, &str) -> Result<(), String>,
    mut poll: impl FnMut() -> Result<(), String>,
) -> Ended {
    let opened = Instant::now();
    let mut last_msg = Instant::now();
    let mut last_poll = Instant::now();
    loop {
        if stop.load(Ordering::Relaxed) {
            let _ = ws.close(None);
            let _ = ws.flush();
            return Ended::Stopped;
        }
        if opened.elapsed() >= max_age {
            let _ = ws.close(None);
            let _ = ws.flush();
            return Ended::Rotate;
        }
        if last_msg.elapsed() >= stale_after {
            return Ended::Stale;
        }
        if last_poll.elapsed() >= Duration::from_secs(1) {
            last_poll = Instant::now();
            if let Err(r) = poll() {
                return Ended::Handler(r);
            }
        }
        match ws.read() {
            Ok(Message::Text(t)) => {
                last_msg = Instant::now();
                if let Err(r) = on_text(wall_ns(), t.as_str()) {
                    return Ended::Handler(r);
                }
            }
            Ok(Message::Binary(b)) => {
                last_msg = Instant::now();
                if let Ok(t) = std::str::from_utf8(&b) {
                    if let Err(r) = on_text(wall_ns(), t) {
                        return Ended::Handler(r);
                    }
                }
            }
            Ok(Message::Close(_)) => return Ended::Closed,
            Ok(_) => {
                // Ping, pong or raw frame: tungstenite queues the pong reply.
                last_msg = Instant::now();
                let _ = ws.flush();
            }
            Err(tungstenite::Error::Io(e))
                if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) =>
            {
                let _ = ws.flush();
            }
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => return Ended::Closed,
            Err(e) => return Ended::Error(super::rest::redact(&e.to_string())),
        }
    }
}

/// Reconnect backoff: doubles from 1 s to 60 s, with jitter from the clock.
#[derive(Debug, Clone)]
pub struct Backoff {
    next_ms: u64,
}

impl Default for Backoff {
    fn default() -> Self {
        Self { next_ms: 1_000 }
    }
}

impl Backoff {
    /// The delay before the next attempt, then double it.
    pub fn next_delay(&mut self) -> Duration {
        let base = self.next_ms;
        self.next_ms = (self.next_ms * 2).min(60_000);
        // Jitter in [50%, 100%) of the base from the wall clock's low bits.
        let jitter = (wall_ns().unsigned_abs() % 500) * base / 1_000;
        Duration::from_millis(base / 2 + jitter)
    }

    /// A connection stayed up: start again from 1 s.
    pub fn reset(&mut self) {
        self.next_ms = 1_000;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_runs_before_any_connection() {
        let guard = HostGuard::new(&["demo-fstream.binance.com"]).unwrap();
        let e = connect("wss://fstream.binance.com/public/stream", &guard, Duration::from_secs(1)).unwrap_err();
        assert!(matches!(e, LiveError::Guard(_)));
        let e = connect("ws://demo-fstream.binance.com/x", &guard, Duration::from_secs(1)).unwrap_err();
        assert!(matches!(e, LiveError::Guard(_)));
    }

    #[test]
    fn backoff_doubles_to_a_cap_and_resets() {
        let mut b = Backoff::default();
        let d: Vec<u128> = (0..9).map(|_| b.next_delay().as_millis()).collect();
        assert!(d[0] >= 500 && d[0] < 1_000);
        assert!(d[8] >= 30_000 && d[8] < 60_000);
        b.reset();
        assert!(b.next_delay().as_millis() < 1_000);
        assert_eq!(Ended::Handler("x".to_owned()).code(), "x");
        assert_eq!(Ended::Rotate.code(), "rotate");
    }
}
