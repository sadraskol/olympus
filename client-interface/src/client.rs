use crate::queries;
use crate::queries::{Answers_AnswerType, Commands_CommandType, WriteAnswer_WriteType};
use protobuf::Message;
use std::fmt::{Debug, Formatter};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub trait Proto<T: Message> {
    fn from_proto(msg: &T) -> Self;
    fn to_proto(&self) -> T;
}

pub async fn write_to<M: Message, P: Proto<M>>(w: &mut TcpStream, s: &P) -> std::io::Result<()> {
    let buf = s.to_proto().write_to_bytes()?;
    w.write_u64(buf.len() as u64).await?;
    w.write_all(&buf).await?;
    Ok(())
}

pub async fn read_from<M: Message, P: Proto<M>>(r: &mut TcpStream) -> std::io::Result<P> {
    let response_size = r.read_u64().await?;
    let mut buf = vec![0; response_size as usize];
    r.read_exact(&mut buf).await?;

    let msg = M::parse_from_bytes(&buf)?;
    Ok(P::from_proto(&msg))
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Key(pub Vec<u8>);

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("Key")
            .field(&std::str::from_utf8(&self.0).unwrap())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Value(pub Vec<u8>);

impl Debug for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("Value")
            .field(&std::str::from_utf8(&self.0).unwrap())
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteResult {
    Rejected,
    Accepted,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Answer {
    Read(Option<Value>),
    Write(WriteResult),
}

impl Proto<queries::Answers> for Answer {
    fn from_proto(msg: &queries::Answers) -> Self {
        match msg.get_field_type() {
            Answers_AnswerType::Read => {
                if msg.get_read().get_is_nil() {
                    Answer::Read(None)
                } else {
                    Answer::Read(Some(Value(msg.get_read().get_value().to_vec())))
                }
            }
            Answers_AnswerType::Write => match msg.get_write().get_code() {
                WriteAnswer_WriteType::Ok => Answer::Write(WriteResult::Accepted),
                WriteAnswer_WriteType::Refused => Answer::Write(WriteResult::Rejected),
            },
        }
    }

    fn to_proto(&self) -> queries::Answers {
        let mut answer = queries::Answers::new();
        match self {
            Answer::Read(r) => {
                answer.set_field_type(queries::Answers_AnswerType::Read);
                let mut read = queries::ReadAnswer::new();
                match r {
                    None => {
                        read.set_is_nil(true);
                    }
                    Some(v) => {
                        read.set_is_nil(false);
                        read.set_value(v.0.clone());
                    }
                }
                answer.set_read(read);
            }
            Answer::Write(w) => {
                answer.set_field_type(queries::Answers_AnswerType::Write);
                let mut write = queries::WriteAnswer::new();
                match w {
                    WriteResult::Rejected => {
                        write.set_code(queries::WriteAnswer_WriteType::Refused);
                    }
                    WriteResult::Accepted => {
                        write.set_code(queries::WriteAnswer_WriteType::Ok);
                    }
                }
                answer.set_write(write);
            }
        }
        answer
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Query {
    Read(Key),
    Write(Key, Value),
    RMW(Key, Value)
}

impl Proto<queries::Commands> for Query {
    fn from_proto(msg: &queries::Commands) -> Self {
        match msg.get_field_type() {
            Commands_CommandType::Read => Query::Read(Key(msg.get_read().get_key().to_vec())),
            Commands_CommandType::Write => if msg.get_write().get_rmw() {
                Query::RMW(
                    Key(msg.get_write().get_key().to_vec()),
                    Value(msg.get_write().get_value().to_vec()),
                )
            } else {
                Query::Write(
                    Key(msg.get_write().get_key().to_vec()),
                    Value(msg.get_write().get_value().to_vec()),
                )
            },
        }
    }

    fn to_proto(&self) -> queries::Commands {
        match self {
            Query::Read(key) => {
                let mut read = queries::Read::new();
                read.set_key(key.0.to_vec());
                let mut command = queries::Commands::new();
                command.set_read(read);
                command.set_field_type(queries::Commands_CommandType::Read);
                command
            }
            Query::Write(key, value) => {
                let mut write = queries::Write::new();
                write.set_key(key.0.to_vec());
                write.set_value(value.0.to_vec());
                write.set_rmw(false);
                let mut command = queries::Commands::new();
                command.set_write(write);
                command.set_field_type(queries::Commands_CommandType::Write);
                command
            }
            Query::RMW(key, value) => {
                let mut write = queries::Write::new();
                write.set_key(key.0.to_vec());
                write.set_value(value.0.to_vec());
                write.set_rmw(true);
                let mut command = queries::Commands::new();
                command.set_write(write);
                command.set_field_type(queries::Commands_CommandType::Write);
                command
            }
        }
    }
}

impl Query {
    pub fn write(key: Key, value: Value) -> Self {
        Query::Write(key, value)
    }

    pub fn read(key: Key) -> Self {
        Query::Read(key)
    }
}
