use {
    crate::{
        error::Error,
        protocol::{Message, MessageBody, MessageIdGenerator, NodeId},
    },
    futures::{future::LocalBoxFuture, FutureExt},
    serde::{de::DeserializeOwned, Serialize},
    std::{collections::HashMap, future::Future, sync::Arc},
};

#[derive(Default, Clone)]
pub struct Node {
    routes: HashMap<
        &'static str,
        Arc<dyn Fn(Context, Message) -> Result<LocalBoxFuture<'static, Result<(), Error>>, Error>>,
    >,
}

#[derive(Debug, Clone)]
pub struct Context(Arc<SharedContext>);

#[derive(Debug)]
struct SharedContext {
    node_id: NodeId,
    other_node_ids: Vec<NodeId>,
    msg_id_generator: MessageIdGenerator,
}

impl Context {
    pub(crate) fn new(node_id: NodeId, other_node_ids: Vec<NodeId>) -> Self {
        Self(Arc::new(SharedContext {
            node_id,
            other_node_ids,
            msg_id_generator: MessageIdGenerator::default(),
        }))
    }

    pub fn node_id(&self) -> &NodeId {
        &self.0.node_id
    }

    pub async fn reply(
        &self,
        message: Message,
        reply_body: impl Into<MessageBody>,
    ) -> Result<(), Error> {
        let mut reply_body = serde_json::to_value(reply_body.into())?;
        reply_body["msg_id"] = serde_json::to_value(self.0.msg_id_generator.next_id())?;

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
}

pub trait Handler<Request, Response> {
    type Future: Future<Output = Result<Response, Error>>;

    fn call(&self, context: Context, request: Request) -> Self::Future;
}

impl Node {
    pub fn add_route<Request, Response>(
        mut self,
        msg_type: &'static str,
        handler: impl Handler<Request, Response> + 'static,
    ) -> Self
    where
        Request: DeserializeOwned + 'static,
        Response: Into<MessageBody> + Serialize + 'static,
    {
        let handler = move |context: Context, message: Message| {
            let request = serde_json::from_value(message.body.clone())?;
            let future = handler.call(context.clone(), request);

            Ok(async move {
                let response = future.await?;

                if message.msg_id().is_some() {
                    context.reply(message, response).await?;
                }

                Ok(())
            }
            .boxed_local())
        };

        self.routes.insert(msg_type, Arc::new(handler));
        self
    }

    pub fn handle(
        &self,
        context: &Context,
        message: Message,
    ) -> Result<LocalBoxFuture<Result<(), Error>>, Error> {
        eprintln!("message: {:?}", message);

        match message
            .msg_type()
            .and_then(|msg_type| self.routes.get(msg_type))
        {
            Some(handler) => handler(context.clone(), message),
            None => Err(Error::NotImplemented),
        }
    }
}

impl<T, Req, Res, Fut> Handler<Req, Res> for T
where
    T: Fn(Context, Req) -> Fut,
    Fut: Future<Output = Result<Res, Error>>,
{
    type Future = Fut;

    fn call(&self, context: Context, request: Req) -> Self::Future {
        self(context, request)
    }
}
