//! The signaling websocket, on whichever websocket the target has.
//!
//! Natively that's `tokio-tungstenite` over rustls, which lets the
//! client set request headers. In a browser it's
//! `tokio-tungstenite-wasm`, i.e. the page's own `WebSocket` — and a
//! browser lets a caller set no headers at all. So on wasm the protocol
//! version rides the query string instead (see [`connect`]).
//!
//! Everything above this module talks in binary frames and doesn't care
//! which of the two is underneath.

use futures_util::SinkExt;

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use super::*;

    /// The concrete stream `tokio_tungstenite::connect_async` hands back.
    pub type Stream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
    pub type Error = tokio_tungstenite::tungstenite::Error;

    /// What came off the socket, with the frames this crate doesn't
    /// care about already dealt with.
    pub enum Frame {
        Binary(Vec<u8>),
        /// A ping (tungstenite has already queued the pong) or anything
        /// else that isn't ours to interpret.
        Ignored,
        Closed,
    }

    pub async fn connect(url: &url::Url, protocol_version: u32) -> Result<Stream, crate::client::Error> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        use tokio_tungstenite::tungstenite::http::HeaderValue;

        let mut req = url.to_string().into_client_request()?;
        req.headers_mut().append(
            "User-Agent",
            HeaderValue::from_str(&format!("tango-signaling/{}", env!("CARGO_PKG_VERSION")))
                .map_err(tokio_tungstenite::tungstenite::http::Error::from)?,
        );
        req.headers_mut().append(
            "X-Tango-Protocol-Version",
            HeaderValue::from_str(&format!("{:x}", protocol_version))
                .map_err(tokio_tungstenite::tungstenite::http::Error::from)?,
        );
        match tokio_tungstenite::connect_async(req).await {
            Ok((stream, _)) => Ok(stream),
            Err(tokio_tungstenite::tungstenite::Error::Http(e)) if e.status() == http::StatusCode::BAD_REQUEST => {
                Err(crate::client::Error::server_abort(
                    e.body().as_ref().map(|b| b.as_bytes()).unwrap_or_default(),
                ))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn send_binary(stream: &mut Stream, bytes: Vec<u8>) -> Result<(), Error> {
        stream.send(tokio_tungstenite::tungstenite::Message::Binary(bytes)).await
    }

    pub fn classify(message: tokio_tungstenite::tungstenite::Message) -> Frame {
        match message {
            tokio_tungstenite::tungstenite::Message::Binary(d) => Frame::Binary(d),
            // Upon receiving a ping, tungstenite queues the pong itself;
            // responding here would double it.
            tokio_tungstenite::tungstenite::Message::Ping(_) => Frame::Ignored,
            tokio_tungstenite::tungstenite::Message::Close(_) => Frame::Closed,
            _ => Frame::Ignored,
        }
    }

    /// Whether an error is a transport-level hiccup a reconnect might
    /// paper over, as opposed to a protocol-level rejection.
    pub fn is_transient(e: &Error) -> bool {
        use tokio_tungstenite::tungstenite::Error as Ws;
        matches!(
            e,
            Ws::ConnectionClosed | Ws::AlreadyClosed | Ws::Io(_) | Ws::Protocol(_) | Ws::Tls(_)
        )
    }

    pub fn closed() -> Error {
        Error::ConnectionClosed
    }

    /// Close the socket politely, ignoring whether the peer is still
    /// there to hear it.
    pub async fn close(stream: &mut Stream) {
        let _ = stream.close(None).await;
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use super::*;

    pub type Stream = tokio_tungstenite_wasm::WebSocketStream;
    pub type Error = tokio_tungstenite_wasm::Error;

    pub enum Frame {
        Binary(Vec<u8>),
        Ignored,
        Closed,
    }

    /// A browser's WebSocket takes a URL and nothing else — no request
    /// headers to put the protocol version in — so the version goes in
    /// the query string.
    pub async fn connect(url: &url::Url, protocol_version: u32) -> Result<Stream, crate::client::Error> {
        let mut url = url.clone();
        url.query_pairs_mut()
            .append_pair("protocol_version", &format!("{:x}", protocol_version));
        Ok(tokio_tungstenite_wasm::connect(url.as_str()).await?)
    }

    pub async fn send_binary(stream: &mut Stream, bytes: Vec<u8>) -> Result<(), Error> {
        stream.send(tokio_tungstenite_wasm::Message::binary(bytes)).await
    }

    pub fn classify(message: tokio_tungstenite_wasm::Message) -> Frame {
        match message {
            tokio_tungstenite_wasm::Message::Binary(d) => Frame::Binary(d.into()),
            // The browser answers pings itself, below this API.
            tokio_tungstenite_wasm::Message::Close(_) => Frame::Closed,
            tokio_tungstenite_wasm::Message::Text(_) => Frame::Ignored,
        }
    }

    pub fn is_transient(e: &Error) -> bool {
        use tokio_tungstenite_wasm::Error as Ws;
        matches!(e, Ws::ConnectionClosed | Ws::AlreadyClosed | Ws::Io(_) | Ws::Protocol(_))
    }

    pub fn closed() -> Error {
        Error::ConnectionClosed
    }

    /// Close the socket politely. The browser has no close-frame
    /// argument to give, so this is `Sink::close`.
    pub async fn close(stream: &mut Stream) {
        use futures_util::SinkExt;
        let _ = SinkExt::close(stream).await;
    }
}

pub use imp::{classify, close, closed, connect, is_transient, send_binary, Error, Frame, Stream};
