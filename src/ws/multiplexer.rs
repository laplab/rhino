//! This module implements a WebSocket multiplexer. In simple words, it allows multiple logical
//! connections to use single WebSocket. By "logical connection" we mean a stream of
//! incoming/outgoing messages, each marked with the same correlation id.
//!
//! Multiplexer is needed mainly to avoid constantly opening and closing connections to the server.
//! Multiplexer also automatically reconnects if the carrier WebSocket connection experiences
//! issues.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use exponential_backoff::Backoff;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{control_server, region_server, Region, RegionInfo, WithCorrelationId};

struct WebsocketMultiplexer<I, E> {
    region: Region,
    address: String,
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    streams: WebsocketStreams<I, E>,
    egress_receiver: UnboundedReceiver<E>,
    // TODO: This grows pretty fast, try LRU or rolling window.
    closed_correlation_ids: HashSet<String>,
    multiplexer_cancel: CancellationToken,
}

#[derive(Debug)]
pub enum WebsocketMultiplexerError {
    UnknownCorrelationId,
    WebsocketUnavailable,
}

impl<I, E> WebsocketMultiplexer<I, E>
where
    for<'de> I: WithCorrelationId + Deserialize<'de> + Send,
    E: WithCorrelationId + Serialize + Send,
{
    pub async fn reconnect(&mut self) {
        self.streams.close().await;

        let mut attempt = 1;
        let backoff = Backoff::new(
            u32::MAX,
            Duration::from_millis(100),
            Duration::from_secs(10),
        );
        for delay in &backoff {
            // TODO: Force re-resolve to connect to the most recent deployment of the service.
            match tokio_tungstenite::connect_async(&self.address).await {
                Ok((stream, _)) => {
                    self.ws_stream = Some(stream);
                    info!(
                        addr = self.address,
                        region = %self.region,
                        ?attempt,
                        "WS multiplexer connected to the remote"
                    );
                    break;
                }
                Err(err) => {
                    error!(
                        addr = self.address,
                        region = %self.region,
                        ?attempt,
                        ?err,
                        retry_in = ?delay,
                        "WS multiplexer failed to connect"
                    );
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
            }
        }

        self.streams.reopen().await;
    }

    pub async fn run(mut self) {
        info!(address = ?self.address, region = ?self.region, "WS multiplexer started");

        'socket_loop: loop {
            let ws_stream = {
                if self.ws_stream.is_none() {
                    self.reconnect().await;
                }
                self.ws_stream.as_mut().unwrap()
            };

            tokio::select! {
                _ = self.multiplexer_cancel.cancelled() => {
                    info!(address = ?self.address, region = %self.region, "WS multiplexer stopped gracefully");
                    return;
                }
                egress_message = self.egress_receiver.recv() => {
                    let egress_message = egress_message.expect("egress sender can never be dropped before multiplexer exits");

                    if self.closed_correlation_ids.contains(egress_message.correlation_id()) {
                        info!(correlation_id = egress_message.correlation_id(), "ignoring a message from a closed correlation_id");
                        continue 'socket_loop;
                    }

                    let serialised = Message::Text(serde_json::to_string(&egress_message).unwrap());
                    if let Err(err) = ws_stream.send(serialised).await {
                        // Note: we don't retry sending the egress message here. This means that if
                        // the caller has sent a message, but did not wait for a confirmation from
                        // the remote host, this message is not guaranteed to be delivered. This is
                        // the same guarantee as with the normal network.
                        error!(?err, region = %self.region, "WS multiplexer failed to send an egress message");
                        self.reconnect().await;
                        continue 'socket_loop;
                    }

                    // Message was sent over the websocket.
                }
                ws_message = ws_stream.next() => {
                    let ws_message = match ws_message {
                        Some(ws_message) => ws_message,
                        None => {
                            error!(region = %self.region, "WS multiplexer socket closed unexpectedly, reconnecting");
                            self.reconnect().await;
                            continue 'socket_loop;
                        }
                    };

                    let ws_message = match ws_message {
                        Ok(ws_message) => ws_message,
                        Err(err) => {
                            error!(?err, region = %self.region, "WS multiplexer encountered an error while reading a message, reconnecting");
                            self.reconnect().await;
                            continue 'socket_loop;
                        }
                    };

                    let text = match ws_message {
                        Message::Text(text) => text,
                        Message::Close(..) => {
                            error!(region = %self.region, "WS multiplexer socket closed unexpectedly, reconnecting");
                            self.reconnect().await;
                            continue 'socket_loop;
                        }
                        _ => {
                            error!(?ws_message, region = %self.region, "WS multiplexer got an unexpected message type");
                            self.reconnect().await;
                            continue 'socket_loop;
                        }
                    };

                    let ingress: I = match serde_json::from_str(&text) {
                        Ok(ingress) => ingress,
                        Err(err) => {
                            error!(?err, region = %self.region, "WS multiplexer detected invalid logical protocol");
                            self.reconnect().await;
                            continue 'socket_loop;
                        }
                    };

                    let correlation_id = ingress.correlation_id().to_string();
                    if self.closed_correlation_ids.contains(&correlation_id) {
                        info!(?correlation_id, region = %self.region, "WS multiplexer ignored a message to a closed correlation_id");
                        continue 'socket_loop;
                    }

                    if self.streams.send_to_stream(ingress).await.is_err() {
                        self.closed_correlation_ids.insert(correlation_id);
                        continue 'socket_loop;
                    }

                    // Message was sent to the corresponding handler.
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct WebsocketStreams<I, E>(Arc<Mutex<StreamsInner<I, E>>>);

struct StreamsInner<I, E> {
    locked_down: bool,
    // TODO: this hashmap grows infinitely, need to clear it sometimes.
    ingress_senders: HashMap<String, UnboundedSender<I>>,
    egress_sender: UnboundedSender<E>,
    multiplexer_cancel: CancellationToken,
}

impl<I: WithCorrelationId, E> WebsocketStreams<I, E> {
    pub async fn create_stream(
        &self,
        correlation_id: String,
    ) -> Result<WebsocketStream<I, E>, WebsocketMultiplexerError> {
        let mut inner = self.0.lock().await;
        if inner.locked_down {
            return Err(WebsocketMultiplexerError::WebsocketUnavailable);
        }

        let (sender, receiver) = unbounded_channel();
        if let Some(..) = inner.ingress_senders.insert(correlation_id, sender) {
            panic!("non-unique correlation_id detected");
        }

        Ok(WebsocketStream {
            ingress_receiver: receiver,
            egress_sender: inner.egress_sender.clone(),
        })
    }

    async fn send_to_stream(&mut self, message: I) -> Result<(), WebsocketMultiplexerError> {
        let mut inner = self.0.lock().await;
        inner
            .ingress_senders
            .get_mut(message.correlation_id())
            .ok_or(WebsocketMultiplexerError::UnknownCorrelationId)
            .map(|sender| {
                sender
                    .send(message)
                    .map_err(|_| WebsocketMultiplexerError::UnknownCorrelationId)
            })?
    }

    async fn close(&self) -> Vec<String> {
        let mut inner = self.0.lock().await;
        inner.locked_down = true;

        // Note: dropping all senders automatically closes corresponding channel.
        inner
            .ingress_senders
            .drain()
            .map(|(correlation_id, _)| correlation_id)
            .collect()
    }

    async fn reopen(&self) {
        let mut inner = self.0.lock().await;
        inner.locked_down = false;
    }

    pub async fn stop(self) {
        let inner = self.0.lock().await;
        inner.multiplexer_cancel.cancel();
    }
}

pub struct WebsocketStream<I, E> {
    ingress_receiver: UnboundedReceiver<I>,
    egress_sender: UnboundedSender<E>,
}

impl<I, E> WebsocketStream<I, E> {
    pub async fn recv(&mut self) -> Result<I, WebsocketMultiplexerError> {
        self.ingress_receiver
            .recv()
            .await
            // Error here means that the corresponding sender was dropped during
            // `WebsocketStreams::close()` operation.
            .ok_or(WebsocketMultiplexerError::WebsocketUnavailable)
    }

    pub fn send(&mut self, message: E) -> Result<(), WebsocketMultiplexerError> {
        self.egress_sender
            .send(message)
            // Error here means that the corresponding receiver was dropped because
            // multiplexer has stopped.
            .map_err(|_| WebsocketMultiplexerError::WebsocketUnavailable)
    }
}

pub fn multiplexed_ws<I, E>(region: Region, address: String) -> WebsocketStreams<I, E>
where
    for<'de> I: WithCorrelationId + Deserialize<'de> + Send + 'static,
    E: WithCorrelationId + Serialize + Send + 'static,
{
    let (egress_sender, egress_receiver) = unbounded_channel();
    let multiplexer_cancel = CancellationToken::new();

    let streams = WebsocketStreams(Arc::new(Mutex::new(StreamsInner {
        locked_down: false,
        ingress_senders: Default::default(),
        egress_sender,
        multiplexer_cancel: multiplexer_cancel.clone(),
    })));

    let multiplexer = WebsocketMultiplexer {
        region,
        address,
        ws_stream: None,
        streams: WebsocketStreams(streams.0.clone()),
        egress_receiver,
        closed_correlation_ids: Default::default(),
        multiplexer_cancel,
    };

    tokio::spawn(multiplexer.run());

    streams
}

pub type RegionalStreams =
    WebsocketStreams<region_server::ClientResponse, region_server::ClientRequest>;

pub type ControlStreams =
    WebsocketStreams<control_server::ClientResponse, control_server::ClientRequest>;

pub fn regional_multiplexed_ws(
    regions: Vec<RegionInfo>,
) -> HashMap<Region, WebsocketStreams<region_server::ClientResponse, region_server::ClientRequest>>
{
    let mut region_streams = HashMap::default();
    for RegionInfo { region, address } in regions {
        let streams = multiplexed_ws::<region_server::ClientResponse, region_server::ClientRequest>(
            region, address,
        );

        if let Some(..) = region_streams.insert(region, streams) {
            panic!("duplicate region definitions");
        }
    }
    region_streams
}
