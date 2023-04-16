use {
    crate::{
        error::Error,
        protocol::{Message, MessageBody, MessageId, MessageIdGenerator, NodeId},
    },
    futures::{
        future::{BoxFuture, LocalBoxFuture},
        FutureExt,
    },
    serde::{de::DeserializeOwned, Serialize},
    std::{
        collections::HashMap,
        future::Future,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    },
    tokio::sync::oneshot,
    tower::Service,
};

pub struct Node<State> {
    state: State,
    router: Router<State>,
}

type HandlerFn<State> = dyn Fn(
    NodeContext,
    State,
    Message,
) -> Result<LocalBoxFuture<'static, Result<(), Error>>, Error>;

pub struct Router<State> {
    routes: HashMap<&'static str, Arc<HandlerFn<State>>>,
}

#[derive(Debug, Clone)]
pub struct NodeContext(Arc<SharedContext>);

#[derive(Debug)]
struct SharedContext {
    node_id: NodeId,
    message_id_generator: MessageIdGenerator,
    unacked_messages: Arc<Mutex<HashMap<MessageId, oneshot::Sender<Result<(), Error>>>>>,
}

impl NodeContext {
    pub(crate) fn new(node_id: NodeId) -> Self {
        Self(Arc::new(SharedContext {
            node_id,
            message_id_generator: MessageIdGenerator::default(),
            unacked_messages: Arc::default(),
        }))
    }

    pub fn node_id(&self) -> NodeId {
        self.0.node_id.clone()
    }

    pub fn send_to(&self, node: NodeId) -> NodeService {
        NodeService {
            dst: node,
            msg_id: None,
            context: self.clone(),
        }
    }

    pub async fn reply(
        &self,
        message: Message,
        reply_body: impl Into<MessageBody>,
    ) -> Result<(), Error> {
        let mut reply_body = serde_json::to_value(reply_body.into())?;
        reply_body["msg_id"] = serde_json::to_value(self.0.message_id_generator.next_id())?;

        if let Some(msg_id) = message.msg_id() {
            reply_body["in_reply_to"] = serde_json::to_value(msg_id)?;
        }

        Message {
            src: self.node_id().clone(),
            dest: message.src,
            body: reply_body,
        }
        .send()
        .await?;

        Ok(())
    }

    fn register(&self, msg_id: MessageId) -> oneshot::Receiver<Result<(), Error>> {
        let (sender, receiver) = oneshot::channel();
        self.0
            .unacked_messages
            .lock()
            .unwrap()
            .insert(msg_id, sender);
        receiver
    }

    fn acknowledge(&self, msg_id: MessageId) {
        if let Some(sender) = self.0.unacked_messages.lock().unwrap().remove(&msg_id) {
            let _ = sender.send(Ok(()));
        }
    }

    fn cancel(&self, msg_id: MessageId) {
        eprintln!("message cancelled: {msg_id:?}");

        if let Some(sender) = self.0.unacked_messages.lock().unwrap().remove(&msg_id) {
            let _ = sender.send(Err(Error::RequestCancelled));
        }
    }
}

pub struct NodeService {
    dst: NodeId,
    msg_id: Option<MessageId>,
    context: NodeContext,
}

impl Service<MessageBody> for NodeService {
    type Response = ();
    type Error = Error;
    type Future = BoxFuture<'static, Result<(), Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: MessageBody) -> Self::Future {
        if let Some(msg_id) = self
            .msg_id
            .replace(self.context.0.message_id_generator.next_id())
        {
            self.context.cancel(msg_id);
        }

        let msg_id = self.msg_id.expect("should have a msg id");

        let receiver = self.context.register(msg_id);

        let src = self.context.node_id();
        let dest = self.dst.clone();
        async move {
            let mut body = serde_json::to_value(req)?;
            body["msg_id"] = serde_json::to_value(msg_id)?;

            Message { src, dest, body }.send().await?;
            let _ = receiver.await;
            Ok(())
        }
        .boxed()
    }
}

impl Drop for NodeService {
    fn drop(&mut self) {
        if let Some(msg_id) = self.msg_id.take() {
            self.context.acknowledge(msg_id);
        }
    }
}

pub trait Handler<Request, Response, State> {
    type Future: Future<Output = Result<Response, Error>>;

    fn call(&self, context: NodeContext, state: State, request: Request) -> Self::Future;
}

impl Default for Node<()> {
    fn default() -> Self {
        Self::with_state(())
    }
}

impl<State> Node<State> {
    pub fn with_state(state: State) -> Self {
        Self {
            state,
            router: Router {
                routes: HashMap::new(),
            },
        }
    }

    pub fn add_handler<Request, Response>(
        mut self,
        msg_type: &'static str,
        handler: impl Handler<Request, Response, State> + 'static,
    ) -> Self
    where
        State: 'static,
        Request: DeserializeOwned + 'static,
        Response: Into<MessageBody> + Serialize + 'static,
    {
        let handler = move |context: NodeContext, state: State, message: Message| {
            let request: Request = serde_json::from_value(message.body.clone())?;
            let future = handler.call(context.clone(), state, request);

            Ok(async move {
                let response = future.await?;

                if message.msg_id().is_some() {
                    context.reply(message, response).await?;
                }

                Ok(())
            }
            .boxed_local())
        };

        self.router.routes.insert(msg_type, Arc::new(handler));
        self
    }

    pub(crate) fn into_parts(self) -> (State, Router<State>) {
        (self.state, self.router)
    }
}

impl<State> Router<State> {
    pub fn handle(
        &self,
        context: &NodeContext,
        state: &State,
        message: Message,
    ) -> Result<LocalBoxFuture<Result<(), Error>>, Error>
    where
        State: Clone + 'static,
    {
        if let Some(in_reply_to) = message.in_reply_to() {
            context.acknowledge(in_reply_to);
        }

        match message
            .msg_type()
            .and_then(|msg_type| self.routes.get(msg_type))
        {
            Some(handler) => handler(context.clone(), state.clone(), message),
            None => Err(Error::NotImplemented),
        }
    }
}

impl<T, Request, Response, State, Fut> Handler<Request, Response, State> for T
where
    T: Fn(NodeContext, State, Request) -> Fut,
    Fut: Future<Output = Result<Response, Error>>,
{
    type Future = Fut;

    fn call(&self, context: NodeContext, state: State, request: Request) -> Self::Future {
        self(context, state, request)
    }
}
