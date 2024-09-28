use std::convert::{TryFrom, TryInto};

use cometbft::abci::request::CheckTxKind;
use cometbft::Time;
use futures::future::{FutureExt, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{FuturesOrdered, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    net::{TcpListener, ToSocketAddrs},
    select,
};

use tokio_util::codec::{FramedRead, FramedWrite};
use tower::{Service, ServiceExt};

use crate::BoxError;
use cometbft::abci::MethodKind;

#[cfg(target_family = "unix")]
use std::path::Path;
#[cfg(target_family = "unix")]
use tokio::net::UnixListener;

use cometbft_proto::abci::v1::Request as ProtoRequest;
use cometbft_proto::abci::v1::Response as ProtoResponse;

use cometbft::abci::v1::{
    ConsensusRequest, ConsensusResponse, InfoRequest, InfoResponse, MempoolRequest,
    MempoolResponse, Request, Response, SnapshotRequest, SnapshotResponse,
};

/// An ABCI server which listens for connections and forwards requests to four
/// component ABCI [`Service`]s.
pub struct Server<C, M, I, S> {
    consensus: C,
    mempool: M,
    info: I,
    snapshot: S,
}

pub struct ServerBuilder<C, M, I, S> {
    consensus: Option<C>,
    mempool: Option<M>,
    info: Option<I>,
    snapshot: Option<S>,
}

impl<C, M, I, S> Default for ServerBuilder<C, M, I, S> {
    fn default() -> Self {
        Self {
            consensus: None,
            mempool: None,
            info: None,
            snapshot: None,
        }
    }
}

impl<C, M, I, S> ServerBuilder<C, M, I, S>
where
    C: Service<ConsensusRequest, Response = ConsensusResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    C::Future: Send + 'static,
    M: Service<MempoolRequest, Response = MempoolResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    M::Future: Send + 'static,
    I: Service<InfoRequest, Response = InfoResponse, Error = BoxError> + Send + Clone + 'static,
    I::Future: Send + 'static,
    S: Service<SnapshotRequest, Response = SnapshotResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    S::Future: Send + 'static,
{
    pub fn consensus(mut self, consensus: C) -> Self {
        self.consensus = Some(consensus);
        self
    }

    pub fn mempool(mut self, mempool: M) -> Self {
        self.mempool = Some(mempool);
        self
    }

    pub fn info(mut self, info: I) -> Self {
        self.info = Some(info);
        self
    }

    pub fn snapshot(mut self, snapshot: S) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    pub fn finish(self) -> Option<Server<C, M, I, S>> {
        let consensus = self.consensus?;
        let mempool = self.mempool?;
        let info = self.info?;
        let snapshot = self.snapshot?;

        Some(Server {
            consensus,
            mempool,
            info,
            snapshot,
        })
    }
}

impl<C, M, I, S> Server<C, M, I, S>
where
    C: Service<ConsensusRequest, Response = ConsensusResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    C::Future: Send + 'static,
    M: Service<MempoolRequest, Response = MempoolResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    M::Future: Send + 'static,
    I: Service<InfoRequest, Response = InfoResponse, Error = BoxError> + Send + Clone + 'static,
    I::Future: Send + 'static,
    S: Service<SnapshotRequest, Response = SnapshotResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    S::Future: Send + 'static,
{
    pub fn builder() -> ServerBuilder<C, M, I, S> {
        ServerBuilder::default()
    }

    #[cfg(target_family = "unix")]
    pub async fn listen_unix(self, path: impl AsRef<Path>) -> Result<(), BoxError> {
        let listener = UnixListener::bind(path)?;
        let addr = listener.local_addr()?;
        tracing::info!(?addr, "ABCI server starting on uds");

        loop {
            match listener.accept().await {
                Ok((socket, _addr)) => {
                    tracing::debug!(?_addr, "accepted new connection");
                    let conn = Connection {
                        consensus: self.consensus.clone(),
                        mempool: self.mempool.clone(),
                        info: self.info.clone(),
                        snapshot: self.snapshot.clone(),
                    };
                    let (read, write) = socket.into_split();
                    tokio::spawn(async move { conn.run(read, write).await.unwrap() });
                }
                Err(e) => {
                    tracing::error!({ %e }, "error accepting new connection");
                }
            }
        }
    }

    pub async fn listen_tcp<A: ToSocketAddrs + std::fmt::Debug>(
        self,
        addr: A,
    ) -> Result<(), BoxError> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        tracing::info!(?addr, "ABCI server starting on tcp socket");

        loop {
            match listener.accept().await {
                Ok((socket, _addr)) => {
                    tracing::debug!(?_addr, "accepted new connection");
                    let conn = Connection {
                        consensus: self.consensus.clone(),
                        mempool: self.mempool.clone(),
                        info: self.info.clone(),
                        snapshot: self.snapshot.clone(),
                    };
                    let (read, write) = socket.into_split();
                    tokio::spawn(async move {
                        if let Err(err) = conn.run(read, write).await {
                            tracing::error!({ %err }, "connection error");
                        }
                    });
                }
                Err(e) => {
                    tracing::error!({ %e }, "error accepting new connection");
                }
            }
        }
    }
}

struct Connection<C, M, I, S> {
    consensus: C,
    mempool: M,
    info: I,
    snapshot: S,
}

impl<C, M, I, S> Connection<C, M, I, S>
where
    C: Service<ConsensusRequest, Response = ConsensusResponse, Error = BoxError> + Send + 'static,
    C::Future: Send + 'static,
    M: Service<MempoolRequest, Response = MempoolResponse, Error = BoxError> + Send + 'static,
    M::Future: Send + 'static,
    I: Service<InfoRequest, Response = InfoResponse, Error = BoxError> + Send + 'static,
    I::Future: Send + 'static,
    S: Service<SnapshotRequest, Response = SnapshotResponse, Error = BoxError> + Send + 'static,
    S::Future: Send + 'static,
{
    // XXX handle errors gracefully
    // figure out how / if to return errors to tendermint
    async fn run(
        mut self,
        read: impl AsyncReadExt + std::marker::Unpin,
        write: impl AsyncWriteExt + std::marker::Unpin,
    ) -> Result<(), BoxError> {
        tracing::info!("listening for requests");

        let (mut request_stream, mut response_sink) = {
            use crate::codec::{Decode, Encode};
            (
                FramedRead::new(read, Decode::<ProtoRequest>::default()),
                FramedWrite::new(write, Encode::<ProtoResponse>::default()),
            )
        };

        let mut responses = FuturesOrdered::new();

        loop {
            select! {
                req = request_stream.next() => {
                    let proto: ProtoRequest = match req.transpose()? {
                        Some(proto) => proto,
                        None => return Ok(()),
                    };

                    // NOTE: this blows up for prepare_proposal, process_proposal, and check_tx. To
                    // fix this, a hack called `from_proto`.

                    // let request = Request::try_from(proto)?;
                    let request = from_proto(proto);

                    tracing::debug!(?request, "new request");
                    match request.kind() {
                        MethodKind::Consensus => {
                            let request = request.try_into().expect("checked kind");
                            let response = self.consensus.ready().await?.call(request);
                            // Need to box here for type erasure
                            responses.push_back(response.map_ok(Response::from).boxed());
                        }
                        MethodKind::Mempool => {
                            let request = request.try_into().expect("checked kind");
                            let response = self.mempool.ready().await?.call(request);
                            responses.push_back(response.map_ok(Response::from).boxed());
                        }
                        MethodKind::Snapshot => {
                            let request = request.try_into().expect("checked kind");
                            let response = self.snapshot.ready().await?.call(request);
                            responses.push_back(response.map_ok(Response::from).boxed());
                        }
                        MethodKind::Info => {
                            let request = request.try_into().expect("checked kind");
                            let response = self.info.ready().await?.call(request);
                            responses.push_back(response.map_ok(Response::from).boxed());
                        }
                        MethodKind::Flush => {
                            // Instead of propagating Flush requests to the application,
                            // handle them here by awaiting all pending responses.
                            tracing::debug!(responses.len = responses.len(), "flushing responses");
                            while let Some(response) = responses.next().await {
                                // XXX: sometimes we might want to send errors to tendermint
                                // https://docs.tendermint.com/v0.32/spec/abci/abci.html#errors
                                tracing::debug!(?response, "flushing response");
                                response_sink.send(response?.into()).await?;
                            }
                            // Now we need to tell Tendermint we've flushed responses
                            response_sink.send(Response::Flush.into()).await?;
                        }
                    }
                }
                rsp = responses.next(), if !responses.is_empty() => {
                    let response = rsp.expect("didn't poll when responses was empty");
                    // XXX: sometimes we might want to send errors to tendermint
                    // https://docs.tendermint.com/v0.32/spec/abci/abci.html#errors
                    tracing::debug!(?response, "sending response");
                    response_sink.send(response?.into()).await?;
                }
            }
        }
    }
}

fn from_proto(req: ProtoRequest) -> Request {
    req.clone().value.map(|value| {
        match value {
            // requests that break
            cometbft_proto::abci::v1::request::Value::CheckTx(check_tx) => {
                // NOTE: type is the field that breaks CheckTx here. The issue is that
                // `CheckTxType` in `cometbft_proto` has three fields.
                //
                // see: https://docs.rs/cometbft-proto/0.1.0-alpha.2/cometbft_proto/abci/v1/enum.CheckTxType.html
                //
                // while `CheckTxKind` in `cometbft` only has 2.
                //
                // see: https://docs.rs/cometbft/0.1.0-alpha.2/cometbft/abci/request/enum.CheckTxKind.html
                //
                // As a temporary, hack, we just consider any number greater than 0 to mean "Recheck".
                Request::CheckTx(cometbft::abci::request::CheckTx {
                    tx: check_tx.tx,
                    kind: if check_tx.r#type > 0 { CheckTxKind::Recheck } else { CheckTxKind::New }
                })
            },
            cometbft_proto::abci::v1::request::Value::PrepareProposal(prepare_proposal) => {
                Request::PrepareProposal(cometbft::abci::request::PrepareProposal {
                    max_tx_bytes: prepare_proposal.max_tx_bytes,
                    txs: prepare_proposal.txs,
                    local_last_commit: prepare_proposal.local_last_commit.map(TryInto::try_into).transpose().unwrap(),
                    misbehavior: prepare_proposal.misbehavior.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>().unwrap(),
                    height: prepare_proposal.height.try_into().unwrap(),
                    time: prepare_proposal.time.map(|time| time.try_into().unwrap()).unwrap_or(Time::unix_epoch()),
                    next_validators_hash: prepare_proposal.next_validators_hash.try_into().unwrap(),

                    // NOTE: this is the field that breaks in the original implementation.
                    //
                    // see: https://docs.rs/cometbft/0.1.0-alpha.2/src/cometbft/account.rs.html#51
                    //
                    // the `abci-cli` never sends any data in the `proposer_address`, and so the
                    // conversion function fails.
                    proposer_address: cometbft::account::Id::new([0; 20]),
                })
            },

            cometbft_proto::abci::v1::request::Value::ProcessProposal(process_proposal) => {
                Request::ProcessProposal(cometbft::abci::request::ProcessProposal {
                    txs: process_proposal.txs,
                    proposed_last_commit: process_proposal.proposed_last_commit.map(TryInto::try_into).transpose().unwrap(),
                    misbehavior: process_proposal.misbehavior.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>().unwrap(),
                    hash: process_proposal.hash.try_into().unwrap(),
                    height: process_proposal.height.try_into().unwrap(),
                    time: process_proposal.time.map(|ts| ts.try_into().unwrap()).unwrap_or(Time::unix_epoch()),
                    next_validators_hash: process_proposal.next_validators_hash.try_into().unwrap(),

                    // NOTE: same as above.
                    proposer_address: cometbft::account::Id::new([0; 20]),
                })
            },

            // requests that work
            _ => Request::try_from(req).unwrap()
        }
    }).expect("no request was passed")
}
