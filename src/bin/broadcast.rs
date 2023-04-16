use {
    gossip_glomers::{
        error::Error,
        node::{Node, NodeContext},
        protocol::{
            Broadcast, BroadcastOk, MessageBody, NodeId, Read, ReadOk, Topology, TopologyOk,
        },
        server::Server,
    },
    serde_json::Value,
    std::{
        collections::HashSet,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::time::sleep,
    tower::ServiceExt,
};

#[derive(Default)]
struct State {
    messages: HashSet<i64>,
}

type SharedState = Arc<Mutex<State>>;

async fn broadcast(
    _: NodeContext,
    state: SharedState,
    Broadcast { message }: Broadcast,
) -> Result<BroadcastOk, Error> {
    let mut state = state.lock().unwrap();

    match message {
        Value::Number(number) => {
            if let Some(number) = number.as_i64() {
                state.messages.insert(number);
            }
        }
        Value::Array(array) => {
            for number in array {
                if let Some(number) = number.as_i64() {
                    state.messages.insert(number);
                }
            }
        }
        _ => {}
    };

    Ok(BroadcastOk {})
}

async fn read(_: NodeContext, state: SharedState, _: Read) -> Result<ReadOk, Error> {
    Ok(ReadOk {
        messages: state.lock().unwrap().messages.iter().copied().collect(),
    })
}

async fn topology(
    context: NodeContext,
    state: SharedState,
    Topology { mut topology }: Topology,
) -> Result<TopologyOk, Error> {
    if let Some(neighbours) = topology.remove(&context.node_id()) {
        for neighbour in neighbours {
            tokio::spawn(gossip_with_neighbour(
                neighbour,
                Arc::clone(&state),
                context.clone(),
            ));
        }
    }
    Ok(TopologyOk {})
}

async fn gossip_with_neighbour(neighbour: NodeId, state: SharedState, context: NodeContext) {
    let mut messages_neighbour_knows = HashSet::<i64>::default();

    loop {
        const TIME_BETWEEN_GOSSIPING: Duration = Duration::from_millis(250);
        sleep(TIME_BETWEEN_GOSSIPING).await;

        let response = context
            .send_to(neighbour.clone())
            .oneshot(Read {}.into())
            .await
            .map(|message| message.body)
            .map(serde_json::from_value);

        if let Ok(Ok(MessageBody::ReadOk(ReadOk {
            messages: Value::Array(messages),
        }))) = response
        {
            for message in messages.iter().filter_map(|value| value.as_i64()) {
                messages_neighbour_knows.insert(message);
            }
        }

        let messages_to_tell_neighbour_about = {
            state
                .lock()
                .unwrap()
                .messages
                .difference(&messages_neighbour_knows)
                .copied()
                .collect::<Vec<_>>()
        };

        if !messages_to_tell_neighbour_about.is_empty() {
            let response = context
                .send_to(neighbour.clone())
                .oneshot(
                    Broadcast {
                        message: messages_to_tell_neighbour_about.clone().into(),
                    }
                    .into(),
                )
                .await
                .map(|message| message.body)
                .map(serde_json::from_value);

            if let Ok(Ok(MessageBody::BroadcastOk(BroadcastOk {}))) = response {
                messages_neighbour_knows.extend(messages_to_tell_neighbour_about);
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let node = Node::with_state(SharedState::default())
        .add_handler("broadcast", broadcast)
        .add_handler("read", read)
        .add_handler("topology", topology);

    Server::default().serve(node).await
}
