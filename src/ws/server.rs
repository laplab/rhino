//! This module implements a WebSocket server abstraction with typed messages. A server is always in
//! a passive position, meaning that it is the client who actively reconnects to the server, not the
//! other way around. Thus, the code is much simpler.

use std::future::Future;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info};

#[macro_export]
macro_rules! recv_or_stop {
    ($stream:expr) => {
        match $stream.recv().await {
            Some(result) => result,
            None => {
                // WS connection was closed or encountered a fatal error.
                return;
            }
        }
    };
}

#[macro_export]
macro_rules! send_or_stop {
    ($stream:expr, $message:expr) => {
        if $stream.send($message).is_err() {
            // WS connection was closed or encountered a fatal error.
            return;
        }
    };
}

#[macro_export]
macro_rules! send_on_error {
    ($maybe_error:expr, $stream:expr, $retry_message:expr) => {
        match $maybe_error {
            Ok(value) => value,
            Err(..) => {
                let _ = $stream.send($retry_message);
                return;
            }
        }
    };
}

pub async fn run_ws_server<I, E, T, F>(address: &str, server_factory: F)
where
    F: Fn(UnboundedReceiver<I>, UnboundedSender<E>) -> T,
    T: Future<Output = ()> + Send + Sync + 'static,
    for<'de> I: Deserialize<'de> + Send + 'static,
    E: Serialize + Send + std::fmt::Debug + 'static,
{
    let listener = TcpListener::bind(address).await.unwrap();

    info!("WS server started on {address:?}");

    // TODO: limit concurrent requests.
    // TODO: ensure that panics are handled appropriately.
    while let Ok((stream, _)) = listener.accept().await {
        let peer = match stream.peer_addr() {
            Ok(peer) => peer,
            Err(err) => {
                error!(
                    ?err,
                    "WS connection closed because peer address is unavailable"
                );
                continue;
            }
        };

        let mut ws_stream = match tokio_tungstenite::accept_async(stream).await {
            Ok(ws_stream) => ws_stream,
            Err(err) => {
                error!(?err, ?peer, "failed to accept WS connection");
                continue;
            }
        };

        info!(?peer, "WS connection accepted");

        let (ingress_sender, ingress_receiver) = unbounded_channel();
        let (egress_sender, mut egress_receiver) = unbounded_channel();

        tokio::spawn(server_factory(ingress_receiver, egress_sender));

        tokio::spawn(async move {
            let result: Result<(), &'static str> = async move {
                loop {
                    tokio::select! {
                        egress = egress_receiver.recv() => {
                            let egress = match egress {
                                Some(egress) => egress,
                                None => {
                                    info!("WS server gracefully shutdown because handler exited");
                                    break;
                                },
                            };

                            let msg = Message::Text(serde_json::to_string(&egress).unwrap());
                            ws_stream
                                .send(msg)
                                .await
                                .map_err(|_| "failed to send WS message")?;
                        }
                        ingress = ws_stream.next() => {
                            let ingress = match ingress {
                                Some(ingress) => ingress,
                                None => {
                                    info!("WS server shutdown because connection was closed");
                                    // `ingress_sender` will be dropped, causing the corresponding
                                    // channel to close automatically, thus notifying the handler.
                                    break;
                                },
                            };

                            let ingress = ingress.map_err(|_| "failed to read WS message").and_then(|ws_message| match ws_message {
                                Message::Text(text) => Ok(text),
                                Message::Close(..) => Err("WS connection gracefully closed"),
                                _ => Err("unexpected WS message type")
                            }).and_then(|text| {
                                serde_json::from_str(&text).map_err(|error| { error!(?error, "failed to parse message"); "failed to parse message"} )
                            })?;

                            if ingress_sender.send(ingress).is_err() {
                                info!("WS server gracefully shutdown because handler exited");
                                break;
                            }
                        }
                    }
                }

                Ok(())
            }.await;

            if let Err(err) = result {
                error!(?err, "WS connection closed because of an error");
            }
        });
    }
}
