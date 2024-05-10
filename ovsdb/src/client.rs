//! TCP/Unix socket based OVSDB client.
use std::collections::HashMap;
use std::path::Path;

use futures::{SinkExt, stream::StreamExt};
use serde::de::DeserializeOwned;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UnixStream},
    sync::{
        mpsc::{self, error::SendError},
        oneshot::{self, error::RecvError},
    },
    task::JoinHandle,
};
use tokio_util::codec::Framed;

use crate::protocol::{method::{
    EchoParams, EchoResult, GetSchemaParams, ListDbsResult, Method, Operation, TransactParams,
}, Request, ResponseResult, Uuid};
use crate::protocol::method::{MonitorCancel, MonitorParams};

use super::{Error, protocol, schema::Schema};

/// Internal synchronization failure
#[derive(Debug)]
pub struct SynchronizationError(String);

impl std::fmt::Display for SynchronizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Synchronication failure: {}", self.0)
    }
}

impl std::error::Error for SynchronizationError {}

impl From<SendError<ClientCommand>> for SynchronizationError {
    fn from(err: SendError<ClientCommand>) -> Self {
        Self(err.to_string())
    }
}

impl From<SendError<ClientRequest>> for SynchronizationError {
    fn from(err: SendError<ClientRequest>) -> Self {
        Self(err.to_string())
    }
}

impl From<RecvError> for SynchronizationError {
    fn from(err: RecvError) -> Self {
        Self(err.to_string())
    }
}

/// The error type for operations performed by the [Client].
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    /// An internal error occurred during synchronization.
    #[error("Failed to deliver command")]
    Internal(#[from] SynchronizationError),
    /// An error occurred trying to establish a connection with the OVSDB server.
    #[error("Failed to establish connection with the server")]
    ConnectionFailed(#[source] std::io::Error),
    /// An error occurred while trying to shutdown the client main loop.
    #[error("Failed to shutdown client")]
    ShutdownError(#[from] tokio::task::JoinError),
    /// A client method was executed, but the client is not connected to OVSDB.
    #[error("Client thread not active")]
    NotRunning,
    /// A response was received from the OVSDB server that could not be processed.
    #[error("Unexpected result received in response object")]
    UnexpectedResult,
    /// An error was encountered while processing send/receive with the OVSDB server.
    #[error("An error occurred when communicating with the server")]
    CommunicationFailure(#[from] protocol::CodecError),
    /// A low-level OVSDB error was encountered.
    #[error("OVSDB error")]
    OvsdbError(#[from] crate::Error),
}
#[derive(Debug)]
enum TxSender {
    One(oneshot::Sender<protocol::Response>),
    Mut(mpsc::Sender<Box<dyn ResponseResult + Send>>),
}

#[derive(Debug)]
struct ClientRequest {
    tx: TxSender,
    request: Request,
}

#[derive(Clone, Copy, Debug)]
enum ClientCommand {
    Shutdown,
}
/// ovsdb monitor context helper
#[derive(Debug)]
pub struct Waiter {
    rx: mpsc::Receiver<Box<dyn ResponseResult + Send>>,
    tx: mpsc::Sender<Box<dyn ResponseResult + Send>>,
    request_sender: mpsc::Sender<ClientRequest>,
    id: protocol::Uuid
}

/// An OVSDB client, used to interact with an OVSDB database server.
///
/// The client is a thin wrapper around the various methods available in the OVSDB protocol.
/// Instantiating a client is done through one of the `connect_` methods.
///
/// # Examples
///
/// ```rust,no_run
/// use std::path::Path;
///
/// use ovsdb::Client;
///
/// # tokio_test::block_on(async {
/// let client = Client::connect_unix(Path::new("/var/run/openvswitch/db.sock"))
///     .await
///     .unwrap();
/// # })
/// ```
#[derive(Debug)]
pub struct Client {
    request_sender: Option<mpsc::Sender<ClientRequest>>,
    command_sender: Option<mpsc::Sender<ClientCommand>>,
    handle: JoinHandle<Result<(), ClientError>>,
}

impl Client {
    fn new(
        request_sender: mpsc::Sender<ClientRequest>,
        command_sender: mpsc::Sender<ClientCommand>,
        handle: JoinHandle<Result<(), ClientError>>,
    ) -> Self {
        Self {
            request_sender: Some(request_sender),
            command_sender: Some(command_sender),
            handle,
        }
    }

    async fn start<T>(stream: T) -> Result<Self, ClientError>
    where
        T: AsyncWriteExt + AsyncReadExt + Send + 'static,
    {
        let (requests_tx, requests_rx) = mpsc::channel(32);
        let (commands_tx, commands_rx) = mpsc::channel(32);

        let handle =
            { tokio::spawn(async move { client_main(requests_rx, commands_rx, stream).await }) };

        Ok(Client::new(requests_tx, commands_tx, handle))
    }

    /// Connect to an OVSDB server via TCP socket.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use ovsdb::client::Client;
    ///
    /// # tokio_test::block_on(async {
    /// let client = Client::connect_tcp("127.0.0.1:6641")
    ///     .await
    ///     .unwrap();
    /// # })
    /// ```
    pub async fn connect_tcp<T>(server_addr: T) -> Result<Self, ClientError>
    where
        T: AsRef<str> + tokio::net::ToSocketAddrs,
    {
        let stream = TcpStream::connect(server_addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;
        Client::start(stream).await
    }

    /// Connect to an OVSDB server via UNIX domain socket.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use std::path::Path;
    ///
    /// use ovsdb::client::Client;
    ///
    /// # tokio_test::block_on(async {
    /// let client = Client::connect_unix(Path::new("/var/run/openvswitch/db.sock"))
    ///     .await
    ///     .unwrap();
    /// # })
    /// ```
    pub async fn connect_unix(socket: &Path) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(socket)
            .await
            .map_err(ClientError::ConnectionFailed)?;
        Client::start(stream).await
    }

    /// Disconnect from the OVSDB server and stop processing messages.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use std::path::Path;
    ///
    /// use ovsdb::Client;
    ///
    /// # tokio_test::block_on(async {
    /// let client = Client::connect_unix(Path::new("/var/run/openvswitch/db.sock"))
    ///     .await
    ///     .unwrap();
    ///
    /// // Perform OVSDB operations
    ///
    /// client.stop().await.unwrap();
    /// # })
    pub async fn stop(mut self) -> Result<(), ClientError> {
        if let Some(sender) = self.command_sender.take() {
            sender
                .send(ClientCommand::Shutdown)
                .await
                .map_err(|e| ClientError::Internal(e.into()))?;
            drop(sender);
        };
        if let Some(sender) = self.request_sender.take() {
            drop(sender);
        }

        self.handle.await?
    }
    /// check client is stoped
    pub fn stoped(&self) -> bool {
        self.handle.is_finished()
    }

    /// Execute a raw OVSDB request, receiving a raw response.
    ///
    /// Under normal circumstances, this method should only be called internally, with clients
    /// preferring one of the purpose-built methods (ie. [`Client::echo`]).  However, if for some reason
    /// those methods are insufficient, raw requests can be made to the database.
    ///
    /// ```rust,no_run
    /// use std::path::Path;
    ///
    /// use serde::Serialize;
    ///
    /// use ovsdb::client::Client;
    /// use ovsdb::protocol::{Request, method::Params, method::Method};
    ///
    /// #[derive(Debug, Serialize)]
    /// struct MyParams {
    ///   values: Vec<i32>,
    /// }
    ///
    /// impl Params for MyParams {}
    ///
    /// let params = MyParams { values: vec![1, 2, 3] };
    /// let request = Request::new(Method::Echo, Some(Box::new(params)));
    ///
    /// # tokio_test::block_on(async {
    /// let client = Client::connect_unix(Path::new("/var/run/openvswitch/db.sock"))
    ///     .await
    ///     .unwrap();
    ///
    /// let result: Option<Vec<i32>> = client.execute(request).await.unwrap();
    /// if let Some(r) = result {
    ///   assert_eq!(r, vec![1, 2, 3]);
    /// }
    /// # })
    /// ```
    pub async fn execute<T>(&self, request: Request) -> Result<Option<T>, ClientError>
    where
        T: DeserializeOwned,
    {
        let (tx, rx) = oneshot::channel();

        match &self.request_sender {
            Some(s) => {
                s.send(ClientRequest { tx: TxSender::One(tx), request })
                    .await
                    .map_err(|e| ClientError::Internal(e.into()))?;
                let res = rx.await.map_err(|e| ClientError::Internal(e.into()))?;
                let r: Option<T> = res.result()?;
                Ok(r)
            }
            None => Err(ClientError::NotRunning),
        }
    }
    /// send need waiting request, and return `Waiter`
    pub async fn wait(&self, request: Request) -> Result<Waiter, ClientError>
    {
        let (tx,rx) = mpsc::channel(16);
        let id = request.id().unwrap().clone();
        match &self.request_sender {
            Some(s) => {
                s.send(ClientRequest { tx: TxSender::Mut(tx.clone()), request })
                    .await
                    .map_err(|e| ClientError::Internal(e.into()))?;

                Ok(Waiter::new(rx, tx, self.request_sender.as_ref().unwrap().clone(), id))
            }
            None => Err(ClientError::NotRunning),
        }
    }

    /// Issues an `echo` request to the OVSDB server.
    ///
    /// On success, the arguments to the request are returned as the result.
    ///
    /// ```rust,no_run
    /// use std::path::Path;
    ///
    /// use ovsdb::client::Client;
    ///
    /// # tokio_test::block_on(async {
    /// let client = Client::connect_unix(Path::new("/var/run/openvswitch/db.sock"))
    ///     .await
    ///     .unwrap();
    ///
    /// let args = vec!["Hello", "OVSDB"];
    /// let result = client.echo(args.clone()).await.unwrap();
    /// assert_eq!(*result, args);
    /// # })
    /// ```
    pub async fn echo<T, I>(&self, args: T) -> Result<EchoResult, ClientError>
    where
        T: IntoIterator<Item = I> + Send,
        I: Into<String> + std::fmt::Debug,
    {
        match self
            .execute(crate::protocol::Request::new(
                Method::Echo,
                Some(Box::new(EchoParams::new(args))),
            ))
            .await?
        {
            Some(data) => Ok(data),
            None => Err(ClientError::UnexpectedResult),
        }
    }

    /// Issues a `list_dbs` request to the OVSDB server.
    ///
    /// On success, a list of databases supported by the server are returned.
    ///
    /// ```rust,no_run
    /// use std::path::Path;
    ///
    /// use ovsdb::Client;
    ///
    /// # tokio_test::block_on(async {
    /// let client = Client::connect_unix(Path::new("/var/run/openvswitch/db.sock"))
    ///     .await
    ///     .unwrap();
    ///
    /// let dbs = client.list_databases().await.unwrap();
    /// println!("available databases: {:#?}", dbs);
    /// # })
    /// ```
    pub async fn list_databases(&self) -> Result<ListDbsResult, ClientError> {
        match self
            .execute(crate::protocol::Request::new(Method::ListDatabases, None))
            .await?
        {
            Some(data) => Ok(data),
            None => Err(ClientError::UnexpectedResult),
        }
    }

    /// Issues a `get_schema` request to the OVSDB server.
    ///
    /// On success, a [Schema] instance is returned matching the OVSDB schema for the specified
    /// database.
    ///
    /// ```rust,no_run
    /// use std::path::Path;
    ///
    /// use ovsdb::Client;
    ///
    /// # tokio_test::block_on(async {
    /// let client = Client::connect_unix(Path::new("/var/run/openvswitch/db.sock"))
    ///     .await
    ///     .unwrap();
    ///
    /// let schema = client.get_schema("Open_vSwitch").await.unwrap();
    /// println!("Open_vSwitch schema: {:#?}", schema);
    /// # })
    /// ```
    pub async fn get_schema<S>(&self, database: S) -> Result<Schema, ClientError>
    where
        S: Into<String>,
    {
        match self
            .execute(crate::protocol::Request::new(
                Method::GetSchema,
                Some(Box::new(GetSchemaParams::new(database))),
            ))
            .await?
        {
            Some(data) => Ok(data),
            None => Err(ClientError::UnexpectedResult),
        }
    }

    /// Issues a `transact` request to the OVSDB server.
    ///
    /// TODO
    pub async fn transact<S, T>(
        &self,
        database: S,
        operations: Vec<Operation>,
    ) -> Result<T, ClientError>
    where
        S: Into<String>,
        T: DeserializeOwned,
    {
        match self
            .execute(crate::protocol::Request::new(
                Method::Transact,
                Some(Box::new(TransactParams::new(database, operations))),
            ))
            .await?
        {
            Some(data) => Ok(data),
            None => Err(ClientError::UnexpectedResult),
        }
    }
    /// Start Monitor Session
    /// # Example
    /// ```rust
    /// use std::collections::HashMap;
    /// use std::path::Path;
    /// use serde_json::Value;
    /// use ovsdb::Client;
    /// use ovsdb::protocol::Uuid;
    /// use ovsdb::protocol::method::{MonitorParams, MonitorRequest, Operation};
    /// # tokio_test::block_on(async {
    /// let client = Client::connect_unix(Path::new("/var/run/openvswitch/db.sock"))
    ///     .await
    ///     .unwrap();
    /// let op = Operation::Select { table: "Bridges".into(), clauses: vec![] };
    /// let req = MonitorRequest { columns: vec!["type"].iter_mut().map(|x|x.to_string()).collect() };
    /// let monid = Uuid::default();
    /// let params = MonitorParams::new("Open_vSwitch", Some(serde_json::to_value(monid.clone()).unwrap()), HashMap::from([("Interface".to_string(), req)]));
    /// let mut waiter = client.monitor(params, monid).await?;
    /// let mut cnt = 0;
    /// loop {
    /// 	match waiter.recv::<Value>() {
    ///    		Ok(rep) => {
    /// 			match rep {
    ///    				None => {println!("connect closed!"); return;}
    ///    				Some(v) => { println!("recv {:?}", v)}
    ///  			}
    /// 			cnt += 1;
    /// 			if cnt > 3 {
    /// 				waiter.close().await;
    /// 			}
    /// 		}
    /// 		Err(e) => {
    /// 			if waiter.closed() {
    ///    				return;
    ///        		}
    /// 			eprintln!("cannot parse to value {:?}", e);
    /// 		}
    ///     }
    /// }
    ///
    ///
    /// # });
    ///
    ///
    /// ```
    pub async fn monitor(
        &self,
        params: MonitorParams,
        monid: Uuid
    ) -> Result<Waiter, ClientError>
    {
        self.wait(crate::protocol::Request::from_id(
            	Some(monid),
                Method::Monitor,
                Some(Box::new(params)),
            )
        ).await
    }
}

async fn client_main<T>(
    mut requests: mpsc::Receiver<ClientRequest>,
    mut commands: mpsc::Receiver<ClientCommand>,
    stream: T,
) -> Result<(), ClientError>
where
    T: AsyncReadExt + AsyncWriteExt,
{
    let (mut writer, mut reader) = Framed::new(stream, protocol::Codec::new()).split();
    let mut channels: HashMap<protocol::Uuid, TxSender> = HashMap::new();

    loop {
        tokio::select! {
            Some(req) = requests.recv() => {
                let request:Request = req.request;
                if let Some(id) = request.id() {
                    channels.insert(id.clone(), req.tx);
                }
                writer.send(request.into()).await?;
            },
            Some(cmd) = commands.recv() => {
                match cmd {
                    ClientCommand::Shutdown => {
                        writer.close().await?;
                        return Ok(());
                        // todo!()
                        // writer.
                        // writer.shutdown().await?;
                    }
                }
            }
            msg = reader.next() => {
                if msg.is_none() {
                    return Err(ClientError::ConnectionFailed(std::io::Error::from(std::io::ErrorKind::ConnectionRefused)))
                }
                let msg = msg.unwrap();
                match msg {
                    Ok(protocol::Message::Response(res)) => {

                        if let Some(id) = res.id().cloned() {
                            let chan = &mut channels;
                            if let Some(tx) = chan.get(&id) {
                                match tx {
                                    TxSender::One(_) => {
                                        if let TxSender::One(sender) = chan.remove(&id).unwrap() {
                                            let _ = sender.send(res);
                                        }
                                    }
                                    TxSender::Mut(sender) => {
                                        match sender.send(Box::new(res)).await {
                                            Ok(_) => {},
                                            Err(_) => { chan.remove(&id) ; }
                                        }
                                    }
                                }
                            }
                        } else {

                        }
                    },
                    Ok(protocol::Message::Request(_req)) => {
                        todo!();
                    },
                    Ok(protocol::Message::Update(update)) => {
                        if let Some(id) = update.id() {
                            let chan = &mut channels;
                            if let Some(tx) = chan.get(&id) {
                                match tx {
                                    TxSender::Mut(sender) => {
                                        match sender.send(Box::new(update)).await {
                                            Ok(_) => {},
                                            Err(_) => { chan.remove(&id) ; }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    Err(e) => {eprintln!("{}", e)}
                }
            },
            else => {
                break;
            }
        }
    }

    Ok(())
}


impl Waiter {
    /// create from Client.wait
    fn new(rx: mpsc::Receiver<Box<dyn ResponseResult + Send>>, tx: mpsc::Sender<Box<dyn ResponseResult + Send>>, request_sender: mpsc::Sender<ClientRequest>, id: protocol::Uuid) -> Self {
    	Self {
            rx,
            tx,
            request_sender,
            id,
        }

    }
    /// recv response from waiter, if client closed, recv `None` result
    pub async fn recv<T>(&mut self) -> Result<Option<T>, ClientError>
        where
            T: DeserializeOwned,
    {
        let rep = self.rx.recv().await;
        match rep {
            None =>  Ok(None),
            Some(res) => {
                let v = res.result_value() ?;
                match v {
                    None => {
                        Err(ClientError::UnexpectedResult)
                    }
                    Some(v) => {
                        let r:Option<T> = serde_json::from_value(v).map_err(Error::ParseError)?;
                        Ok(r)
                    }
                }

            }
        }
    }
    /// send close request and waiting done
    pub async fn close(&mut self) {
        let monid = Box::new(MonitorCancel::new(serde_json::to_value(self.id).unwrap()));
        let _ = self.request_sender.send(
            ClientRequest {
                tx: TxSender::Mut(self.tx.clone()),
                request: Request::from_id(Some(self.id),
                                          Method::MonitorCancel,
                                          Some(monid)
                )
            }
        ).await;
        _ = self.rx.recv().await;
        self.rx.close()
    }
    /// check waiter closed on client process main loop
    pub fn closed(&self) -> bool {
        self.rx.is_closed()
    }
}