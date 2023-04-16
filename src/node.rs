use {
    crate::{
        error::Error,
        protocol::{Message, MessageBody, MessageId, NodeId},
    },
    futures::{future::BoxFuture, FutureExt, Stream, StreamExt},
    serde_json::Value,
    std::{
        collections::{HashMap, HashSet},
        pin::pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    },
    tokio::sync::oneshot,
    tower::Service,
    uuid::Uuid,
};

pub struct Node {
    node_id: NodeId,
    other_node_ids: Vec<NodeId>,
    neighbours: Vec<NodeId>,
    seen_broadcast_messages: HashSet<i32>,
    messages: Arc<Messages>,
}

#[derive(Default)]
struct Messages {
    next_msg_id: Mutex<MessageId>,
    unacked_messages: Mutex<HashMap<MessageId, oneshot::Sender<()>>>,
}

impl Node {
    pub fn new(node_id: NodeId, other_node_ids: Vec<NodeId>) -> Self {
        Self {
            node_id,
            other_node_ids,
            neighbours: Vec::default(),
            seen_broadcast_messages: HashSet::default(),
            messages: Arc::default(),
        }
    }

    pub async fn run(mut self, messages: impl Stream<Item = Message>) -> Result<(), Error> {
        let mut messages = pin!(messages);

        while let Some(message) = messages.next().await {
            self.handle_message(message).await?;
        }

        Ok(())
    }

    fn send_to_node(&self, destination_node: NodeId) -> NodeService {
        NodeService {
            source: self.node_id.clone(),
            destination: destination_node,
            messages: Arc::clone(&self.messages),
        }
    }

    async fn handle_message(&mut self, message: Message) -> Result<(), Error> {
        let Message { src, dest, body } = message;

        let response = match body {
            MessageBody::Echo { echo, msg_id } => Some(MessageBody::EchoOk {
                echo,
                msg_id: self.messages.next_msg_id(),
                in_reply_to: msg_id,
            }),
            MessageBody::Generate { msg_id } => Some(MessageBody::GenerateOk {
                id: Uuid::new_v4(),
                msg_id: self.messages.next_msg_id(),
                in_reply_to: msg_id,
            }),
            MessageBody::Broadcast { message, msg_id } => {
                if self.seen_broadcast_messages.insert(message) {
                    for neighbour in &self.neighbours {
                        self.send_to_node(neighbour.clone())
                            .call(MessageBody::Broadcast {
                                message: 0,
                                msg_id: None,
                            });
                    }
                }

                msg_id.map(|msg_id| MessageBody::BroadcastOk {
                    msg_id: self.messages.next_msg_id(),
                    in_reply_to: msg_id,
                })
            }
            MessageBody::BroadcastOk { in_reply_to, .. } => {
                self.messages.acknowledge(in_reply_to);
                None
            }
            MessageBody::Read { msg_id } => Some(MessageBody::ReadOk {
                messages: self.seen_broadcast_messages.iter().copied().collect(),
                msg_id: self.messages.next_msg_id(),
                in_reply_to: msg_id,
            }),
            MessageBody::Topology {
                msg_id,
                mut topology,
            } => {
                if let Some(neighbours) = topology.remove(&self.node_id) {
                    self.neighbours = neighbours;
                }

                Some(MessageBody::TopologyOk {
                    msg_id: self.messages.next_msg_id(),
                    in_reply_to: msg_id,
                })
            }
            _ => None,
        };

        if let Some(response) = response {
            Message {
                src: dest,
                dest: src,
                body: response,
            }
            .send()
            .await?;
        }

        Ok(())
    }
}

impl Messages {
    fn next_msg_id(&self) -> MessageId {
        self.next_msg_id.lock().unwrap().increment()
    }

    fn register(&self, msg_id: MessageId) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        self.unacked_messages.lock().unwrap().insert(msg_id, sender);
        receiver
    }

    fn acknowledge(&self, msg_id: MessageId) {
        let message = { self.unacked_messages.lock().unwrap().remove(&msg_id) };

        if let Some(message) = message {
            let _ = message.send(());
        }
    }

    fn cancel(&self, msg_id: MessageId) {
        let _ = self.unacked_messages.lock().unwrap().remove(&msg_id);
    }
}

struct NodeService {
    source: NodeId,
    destination: NodeId,
    messages: Arc<Messages>,
}

impl Service<MessageBody> for NodeService {
    type Response = ();
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, message: MessageBody) -> Self::Future {
        let message = Message {
            src: self.source.clone(),
            dest: self.destination.clone(),
            body: message,
        };

        let msg_id = message.msg_id().expect("message should have an id");

        let messages = Arc::clone(&self.messages);
        async move {
            let receiver = messages.register(msg_id);
            let result = message.send().await;

            match result {
                Ok(_) => {
                    receiver.await.map_err(|_err| Error::NoResponse(msg_id))?;
                    Ok(())
                }
                Err(err) => {
                    messages.cancel(msg_id);
                    Err(err)
                }
            }
        }
        .boxed()
    }
}
