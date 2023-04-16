use {
    crate::error::Error,
    serde::{Deserialize, Serialize},
    serde_json::Value,
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
    },
    tokio::io::{self, AsyncWriteExt},
    uuid::Uuid,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    pub fn is_server_node(&self) -> bool {
        self.0.starts_with('n')
    }

    pub fn is_client_node(&self) -> bool {
        self.0.starts_with('c')
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct MessageId(u64);

#[derive(Debug, Default, Clone)]
pub struct MessageIdGenerator(Arc<AtomicU64>);

impl MessageIdGenerator {
    pub fn next_id(&self) -> MessageId {
        let MessageIdGenerator(value) = self;
        MessageId(value.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub src: NodeId,
    pub dest: NodeId,
    pub body: Value,
}

impl Message {
    pub fn msg_type(&self) -> Option<&str> {
        self.body.get("type").and_then(Value::as_str)
    }

    pub fn msg_id(&self) -> Option<MessageId> {
        self.body
            .get("msg_id")
            .and_then(Value::as_u64)
            .map(MessageId)
    }

    pub fn in_reply_to(&self) -> Option<MessageId> {
        self.body
            .get("in_reply_to")
            .and_then(Value::as_u64)
            .map(MessageId)
    }

    pub async fn send(self) -> Result<(), Error> {
        let mut json = serde_json::to_string(&self)?;
        json.push('\n');

        let mut std_out = io::stdout();
        std_out.write_all(json.as_bytes()).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Echo {
    pub echo: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EchoOk {
    pub echo: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Generate {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateOk {
    pub id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Broadcast {
    pub message: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Read {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOk {
    pub messages: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    pub topology: HashMap<NodeId, Vec<NodeId>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MessageBody {
    #[serde(rename = "init")]
    Init(Init),

    #[serde(rename = "init_ok")]
    InitOk(InitOk),

    #[serde(rename = "echo")]
    Echo(Echo),

    #[serde(rename = "echo_ok")]
    EchoOk(EchoOk),

    #[serde(rename = "generate")]
    Generate(Generate),

    #[serde(rename = "generate_ok")]
    GenerateOk(GenerateOk),

    #[serde(rename = "broadcast")]
    Broadcast(Broadcast),

    #[serde(rename = "broadcast_ok")]
    BroadcastOk(BroadcastOk),

    #[serde(rename = "read")]
    Read(Read),

    #[serde(rename = "read_ok")]
    ReadOk(ReadOk),

    #[serde(rename = "topology")]
    Topology(Topology),

    #[serde(rename = "topology_ok")]
    TopologyOk(TopologyOk),
}

impl From<Init> for MessageBody {
    fn from(init: Init) -> Self {
        Self::Init(init)
    }
}

impl From<InitOk> for MessageBody {
    fn from(init_ok: InitOk) -> Self {
        Self::InitOk(init_ok)
    }
}

impl From<Echo> for MessageBody {
    fn from(echo: Echo) -> Self {
        Self::Echo(echo)
    }
}

impl From<EchoOk> for MessageBody {
    fn from(echo_ok: EchoOk) -> Self {
        Self::EchoOk(echo_ok)
    }
}

impl From<Generate> for MessageBody {
    fn from(generate: Generate) -> Self {
        Self::Generate(generate)
    }
}

impl From<GenerateOk> for MessageBody {
    fn from(generate_ok: GenerateOk) -> Self {
        Self::GenerateOk(generate_ok)
    }
}

impl From<Broadcast> for MessageBody {
    fn from(broadcast: Broadcast) -> Self {
        Self::Broadcast(broadcast)
    }
}

impl From<BroadcastOk> for MessageBody {
    fn from(broadcast_ok: BroadcastOk) -> Self {
        Self::BroadcastOk(broadcast_ok)
    }
}

impl From<Read> for MessageBody {
    fn from(read: Read) -> Self {
        Self::Read(read)
    }
}

impl From<ReadOk> for MessageBody {
    fn from(read_ok: ReadOk) -> Self {
        Self::ReadOk(read_ok)
    }
}

impl From<Topology> for MessageBody {
    fn from(topology: Topology) -> Self {
        Self::Topology(topology)
    }
}

impl From<TopologyOk> for MessageBody {
    fn from(topology_ok: TopologyOk) -> Self {
        Self::TopologyOk(topology_ok)
    }
}
