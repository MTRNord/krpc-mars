//! Client to the KRPC Stream server.
use crate::codec;
use crate::error;
use crate::krpc;

use crate::client::CallHandle;

use std::marker::PhantomData;

use std::collections::HashMap;

use bytes::BytesMut;
use protobuf::Message;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;
use tracing::warn;

pub(crate) type StreamID = u64;

/// A client to the Stream server.
#[derive(Debug)]
pub struct StreamClient {
    sock: TcpStream,
}

/// A handle to a stream. The type parameter is the type of the value produced by the stream.
#[derive(Copy, Clone, Debug)]
pub struct StreamHandle<T> {
    pub(crate) stream_id: StreamID,
    _phantom: PhantomData<T>,
}

impl<T> StreamHandle<T> {
    #[doc(hidden)]
    /// Creates a new StreamHande. The function is public so that the generated code from
    /// krpc-mars-terraformer can use it but it is hidden from user docs.
    pub fn new(stream_id: StreamID) -> Self {
        StreamHandle {
            stream_id,
            _phantom: PhantomData,
        }
    }

    /// Creates an RPC request that will remove this stream.
    pub fn remove(self) -> CallHandle<()> {
        use codec::RPCEncodable;

        let mut arg = krpc::Argument::new();
        arg.position = 0;
        arg.value = self.stream_id.encode_to_bytes().unwrap();

        let arguments: Vec<krpc::Argument> = vec![arg];

        let mut proc_call = krpc::ProcedureCall::new();
        proc_call.service = String::from("KRPC");
        proc_call.procedure = String::from("RemoveStream");
        proc_call.arguments = arguments;

        CallHandle::<()>::new(proc_call)
    }
}

/// Creates a stream request from a CallHandle. For less verbosity, you can use the
/// [`CallHandle::to_stream`] instead.
///
/// Note that types don't prevent you from chaining multiple `mk_stream`. This will build a stream
/// request of a stream request. Turns out this is accepted by the RPC server and the author of
/// this library confesses he had some fun with this.
pub fn mk_stream<T: codec::RPCExtractable>(call: &CallHandle<T>) -> CallHandle<StreamHandle<T>> {
    let mut arg = krpc::Argument::new();
    arg.position = 0;
    arg.value = call.get_call().write_to_bytes().unwrap();

    let arguments: Vec<krpc::Argument> = vec![arg];

    let mut proc_call = krpc::ProcedureCall::new();
    proc_call.service = String::from("KRPC");
    proc_call.procedure = String::from("AddStream");
    proc_call.arguments = arguments;

    CallHandle::<StreamHandle<T>>::new(proc_call)
}

impl StreamClient {
    /// Connect to the stream server associated with the given client.
    pub async fn connect<A: ToSocketAddrs>(
        client: &super::RPCClient,
        addr: A,
    ) -> Result<Self, error::ConnectionError> {
        let mut sock = TcpStream::connect(addr).await?;

        let mut conn_req = krpc::ConnectionRequest::new();
        conn_req.type_ = krpc::connection_request::Type::STREAM.into();
        conn_req.client_identifier = client.client_id.clone();

        let data = conn_req.write_to_bytes()?;
        sock.write_all(&data).await?;

        sock.readable().await?;

        let mut buffer = BytesMut::with_capacity(1024);
        sock.read_buf(&mut buffer).await?;

        let response = codec::read_message::<krpc::ConnectionResponse>(&buffer.freeze())?;

        match response.status.enum_value() {
            Ok(krpc::connection_response::Status::OK) => Ok(Self { sock }),
            Ok(s) => Err(error::ConnectionError::ConnectionRefused {
                error: response.message,
                status: s,
            }),
            Err(err) => Err(error::ConnectionError::UnknownError {
                error: err.to_string(),
            }),
        }
    }

    pub async fn recv_update(&mut self) -> Result<StreamUpdate, error::RPCError> {
        let mut map = HashMap::new();
        self.sock.readable().await?;

        let mut buffer = BytesMut::with_capacity(4096);
        self.sock.read_buf(&mut buffer).await?;
        if let Ok(updates) = codec::read_message::<krpc::StreamUpdate>(&buffer.freeze()) {
            for mut result in updates.results.into_iter() {
                let taken_result = result.result.take();
                if let Some(inner_result) = taken_result {
                    map.insert(result.id, inner_result);
                }
            }
        } else {
            warn!("Error Message on proto. Ignoring")
        }

        Ok(StreamUpdate { updates: map })
    }
}

/// A collection of updates received from the stream server.
#[derive(Debug, Clone, Default)]
pub struct StreamUpdate {
    updates: HashMap<StreamID, krpc::ProcedureResult>,
}

impl StreamUpdate {
    pub fn get_result<T>(&self, handle: &StreamHandle<T>) -> Result<Option<T>, error::RPCError>
    where
        T: codec::RPCExtractable,
    {
        if let Some(result) = self.updates.get(&handle.stream_id) {
            let res = codec::extract_result(result)?;
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    /// Merge two update objects. The Stream server doesn't update values that don't change, so
    /// this can be used to retain previous values of streams.
    pub fn merge_with(&mut self, other: StreamUpdate) {
        self.updates.extend(other.updates)
    }
}
