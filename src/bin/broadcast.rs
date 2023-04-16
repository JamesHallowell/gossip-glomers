use {
    futures::future::{BoxFuture, FutureExt},
    gossip_glomers::{
        error::Error,
        node::{Node, NodeContext},
        protocol::{
            Broadcast, BroadcastOk, MessageBody, NodeId, Read, ReadOk, Topology, TopologyOk,
        },
        server::Server,
    },
    std::{cell::RefCell, collections::HashSet, rc::Rc, time::Duration},
    tokio::time::sleep,
    tower::{retry::Policy, ServiceBuilder, ServiceExt},
};

#[derive(Default)]
struct State {
    messages: HashSet<i64>,
    neighbours: Vec<NodeId>,
}

type SharedState = Rc<RefCell<State>>;

#[derive(Clone)]
struct ExponentialBackoff {
    attempts_remaining: usize,
    delay: Duration,
}

impl ExponentialBackoff {
    fn with_number_of_attempts(attempts: usize) -> Self {
        Self {
            attempts_remaining: attempts,
            delay: Duration::from_secs(1),
        }
    }

    fn with_starting_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

impl<E> Policy<MessageBody, (), E> for ExponentialBackoff {
    type Future = BoxFuture<'static, Self>;

    fn retry(&self, _: &MessageBody, result: Result<&(), &E>) -> Option<Self::Future> {
        match result {
            Ok(_) => None,
            Err(_) => (self.attempts_remaining > 0).then_some({
                let Self {
                    attempts_remaining,
                    delay,
                } = *self;

                eprintln!(
                    "timeout occurred, retrying in {}ms, {} attempts remain",
                    delay.as_millis(),
                    attempts_remaining
                );

                async move {
                    sleep(delay).await;
                    Self {
                        attempts_remaining: attempts_remaining - 1,
                        delay: delay * 2,
                    }
                }
                .boxed()
            }),
        }
    }

    fn clone_request(&self, req: &MessageBody) -> Option<MessageBody> {
        Some(req.clone())
    }
}

async fn broadcast(
    context: NodeContext,
    state: SharedState,
    Broadcast { message }: Broadcast,
) -> Result<BroadcastOk, Error> {
    let is_new_message = { state.borrow_mut().messages.insert(message) };

    if is_new_message {
        let neighbouring_nodes = { state.borrow().neighbours.clone() };
        for neighbour in neighbouring_nodes {
            tokio::spawn({
                let context = context.clone();

                ServiceBuilder::new()
                    .retry(
                        ExponentialBackoff::with_number_of_attempts(10)
                            .with_starting_delay(Duration::from_millis(50)),
                    )
                    .buffer(1)
                    .timeout(Duration::from_secs(1))
                    .service(context.send_to(neighbour))
                    .oneshot(Broadcast { message }.into())
            });
        }
    }
    Ok(BroadcastOk {})
}

async fn read(_: NodeContext, state: SharedState, _: Read) -> Result<ReadOk, Error> {
    Ok(ReadOk {
        messages: state.borrow().messages.iter().copied().collect(),
    })
}

async fn topology(
    context: NodeContext,
    state: SharedState,
    Topology { mut topology }: Topology,
) -> Result<TopologyOk, Error> {
    if let Some(neighbours) = topology.remove(&context.node_id()) {
        state.borrow_mut().neighbours = neighbours;
    }
    Ok(TopologyOk {})
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let node = Node::with_state(SharedState::default())
        .add_handler("broadcast", broadcast)
        .add_handler("read", read)
        .add_handler("topology", topology);

    Server::default().serve(node).await
}
