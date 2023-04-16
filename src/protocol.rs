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

impl Into<MessageBody> for InitOk {
    fn into(self) -> MessageBody {
        MessageBody::InitOk(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Echo {
    pub echo: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EchoOk {
    pub echo: Value,
}

impl Into<MessageBody> for EchoOk {
    fn into(self) -> MessageBody {
        MessageBody::EchoOk(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Generate {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateOk {
    pub id: Uuid,
}

impl Into<MessageBody> for GenerateOk {
    fn into(self) -> MessageBody {
        MessageBody::GenerateOk(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Broadcast {
    pub message: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastOk {}

impl Into<MessageBody> for BroadcastOk {
    fn into(self) -> MessageBody {
        MessageBody::BroadcastOk(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Read {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOk {
    pub messages: Vec<i32>,
}

impl Into<MessageBody> for ReadOk {
    fn into(self) -> MessageBody {
        MessageBody::ReadOk(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    pub topology: HashMap<NodeId, Vec<NodeId>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyOk {}

impl Into<MessageBody> for TopologyOk {
    fn into(self) -> MessageBody {
        MessageBody::TopologyOk(self)
    }
}

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
