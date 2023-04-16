use {
    crate::error::Error,
    serde::{Deserialize, Serialize},
    serde_json::Value,
    std::collections::HashMap,
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
pub struct MessageId(i32);

impl MessageId {
    pub fn increment(&mut self) -> MessageId {
        let MessageId(value) = self;
        *value += 1;
        *self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub src: NodeId,
    pub dest: NodeId,
    pub body: MessageBody,
}

impl Message {
    pub fn msg_id(&self) -> Option<MessageId> {
        match self.body {
            MessageBody::Init { msg_id, .. } => Some(msg_id),
            MessageBody::Echo { msg_id, .. } => Some(msg_id),
            MessageBody::Generate { msg_id } => Some(msg_id),
            MessageBody::Broadcast { msg_id, .. } => msg_id,
            MessageBody::Read { msg_id } => Some(msg_id),
            MessageBody::Topology { msg_id, .. } => Some(msg_id),
            _ => None,
        }
    }

    pub fn in_reply_to(&self) -> Option<MessageId> {
        match self.body {
            MessageBody::InitOk { in_reply_to } => Some(in_reply_to),
            MessageBody::EchoOk { in_reply_to, .. } => Some(in_reply_to),
            MessageBody::GenerateOk { in_reply_to, .. } => Some(in_reply_to),
            MessageBody::BroadcastOk { in_reply_to, .. } => Some(in_reply_to),
            MessageBody::ReadOk { in_reply_to, .. } => Some(in_reply_to),
            MessageBody::TopologyOk { in_reply_to, .. } => Some(in_reply_to),
            _ => None,
        }
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
#[serde(tag = "type")]
pub enum MessageBody {
    #[serde(rename = "init")]
    Init {
        msg_id: MessageId,
        node_id: NodeId,
        node_ids: Vec<NodeId>,
    },

    #[serde(rename = "init_ok")]
    InitOk { in_reply_to: MessageId },

    #[serde(rename = "echo")]
    Echo { echo: Value, msg_id: MessageId },

    #[serde(rename = "echo_ok")]
    EchoOk {
        echo: Value,
        msg_id: MessageId,
        in_reply_to: MessageId,
    },

    #[serde(rename = "generate")]
    Generate { msg_id: MessageId },

    #[serde(rename = "generate_ok")]
    GenerateOk {
        id: Uuid,
        msg_id: MessageId,
        in_reply_to: MessageId,
    },

    #[serde(rename = "broadcast")]
    Broadcast {
        message: i32,
        msg_id: Option<MessageId>,
    },

    #[serde(rename = "broadcast_ok")]
    BroadcastOk {
        msg_id: MessageId,
        in_reply_to: MessageId,
    },

    #[serde(rename = "read")]
    Read { msg_id: MessageId },

    #[serde(rename = "read_ok")]
    ReadOk {
        messages: Vec<i32>,
        msg_id: MessageId,
        in_reply_to: MessageId,
    },

    #[serde(rename = "topology")]
    Topology {
        topology: HashMap<NodeId, Vec<NodeId>>,
        msg_id: MessageId,
    },

    #[serde(rename = "topology_ok")]
    TopologyOk {
        msg_id: MessageId,
        in_reply_to: MessageId,
    },
}
