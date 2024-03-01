//! Example ABCI application, an in-memory key-value store.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::future::FutureExt;
use structopt::StructOpt;
use tower::{Service, ServiceBuilder};

use tendermint::{
    abci::{
        response::{self, PrepareProposal},
        Event, EventAttributeIndexExt,
    },
    v0_38::abci::request,
};

use tower_abci::{
    v038::{split, Server},
    BoxError,
};

use tendermint::abci::types::ExecTxResult;
use tendermint::v0_38::abci::{Request, Response};

/// In-memory, hashmap-backed key-value store ABCI application.
#[derive(Clone, Debug, Default)]
pub struct KVStore {
    store: HashMap<String, String>,
    height: u32,
    app_hash: [u8; 8],
}

impl Service<Request> for KVStore {
    type Response = Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Response, BoxError>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        tracing::info!(?req);

        let rsp = match req {
            // handled messages
            Request::Info(_) => Response::Info(self.info()),
            Request::Query(query) => Response::Query(self.query(query.data)),
            // Note: https://github.com/tendermint/tendermint/blob/v0.38.x/spec/abci/abci%2B%2B_tmint_expected_behavior.md#adapting-existing-applications-that-use-abci
            Request::PrepareProposal(prepare_prop) => Response::PrepareProposal(PrepareProposal {
                txs: prepare_prop.txs,
            }),
            Request::ProcessProposal(..) => {
                Response::ProcessProposal(response::ProcessProposal::Accept)
            }
            Request::ExtendVote(vote) => Response::ExtendVote(self.extend_vote(vote)),
            Request::VerifyVoteExtension(vote) => {
                Response::VerifyVoteExtension(self.verify_vote(vote))
            }
            Request::FinalizeBlock(block) => Response::FinalizeBlock(self.finalize_block(block)),
            Request::Commit => Response::Commit(self.commit()),

            // unhandled messages
            Request::Flush => Response::Flush,
            Request::Echo(_) => Response::Echo(Default::default()),
            Request::InitChain(_) => Response::InitChain(Default::default()),
            Request::CheckTx(_) => Response::CheckTx(Default::default()),
            Request::ListSnapshots => Response::ListSnapshots(Default::default()),
            Request::OfferSnapshot(_) => Response::OfferSnapshot(Default::default()),
            Request::LoadSnapshotChunk(_) => Response::LoadSnapshotChunk(Default::default()),
            Request::ApplySnapshotChunk(_) => Response::ApplySnapshotChunk(Default::default()),
        };
        tracing::info!(?rsp);
        async move { Ok(rsp) }.boxed()
    }
}

impl KVStore {
    fn info(&self) -> response::Info {
        response::Info {
            data: "tower-abci-kvstore-example".to_string(),
            version: "0.1.0".to_string(),
            app_version: 1,
            last_block_height: self.height.into(),
            last_block_app_hash: self.app_hash.to_vec().try_into().unwrap(),
        }
    }

    fn query(&self, query: Bytes) -> response::Query {
        let key = String::from_utf8(query.to_vec()).unwrap();
        let (value, log) = match self.store.get(&key) {
            Some(value) => (value.clone(), "exists".to_string()),
            None => ("".to_string(), "does not exist".to_string()),
        };

        response::Query {
            log,
            key: key.into_bytes().into(),
            value: value.into_bytes().into(),
            ..Default::default()
        }
    }

    fn execute_tx(&mut self, tx: Bytes) -> ExecTxResult {
        let tx = String::from_utf8(tx.to_vec()).unwrap();
        let tx_parts = tx.split('=').collect::<Vec<_>>();
        let (key, value) = match (tx_parts.first(), tx_parts.get(1)) {
            (Some(key), Some(value)) => (*key, *value),
            _ => (tx.as_ref(), tx.as_ref()),
        };
        self.store.insert(key.to_string(), value.to_string());

        ExecTxResult {
            events: vec![Event::new(
                "app",
                vec![
                    ("key", key).index(),
                    ("index_key", "index is working").index(),
                    ("noindex_key", "noindex is working").no_index(),
                ],
            )],
            ..Default::default()
        }
    }

    fn finalize_block(&mut self, block: request::FinalizeBlock) -> response::FinalizeBlock {
        let mut tx_results = Vec::new();
        for tx in block.txs {
            tx_results.push(self.execute_tx(tx));
        }
        response::FinalizeBlock {
            events: vec![Event::new(
                "app",
                vec![("num_tx", format!("{}", tx_results.len())).index()],
            )],
            tx_results,
            validator_updates: vec![],
            consensus_param_updates: None,
            app_hash: self
                .compute_apphash()
                .to_vec()
                .try_into()
                .expect("vec to `AppHash` conversion is actually infaillible."),
        }
    }

    fn commit(&mut self) -> response::Commit {
        let retain_height = self.height.into();
        // As in the other kvstore examples, just use store.len() as the "hash"
        self.app_hash = self.compute_apphash();
        self.height += 1;

        response::Commit {
            // This field is ignored for CometBFT >= 0.38
            data: Bytes::default(),
            retain_height,
        }
    }

    fn extend_vote(&self, _vote: request::ExtendVote) -> response::ExtendVote {
        response::ExtendVote {
            vote_extension: Bytes::default(),
        }
    }

    fn verify_vote(&self, _vote: request::VerifyVoteExtension) -> response::VerifyVoteExtension {
        response::VerifyVoteExtension::Accept
    }

    fn compute_apphash(&self) -> [u8; 8] {
        (self.store.len() as u64).to_be_bytes()
    }
}

#[derive(Debug, StructOpt)]
struct Opt {
    /// Bind the TCP server to this host.
    #[structopt(short, long, default_value = "127.0.0.1")]
    host: String,

    /// Bind the TCP server to this port.
    #[structopt(short, long, default_value = "26658")]
    port: u16,

    /// Bind the UDS server to this path
    #[structopt(long)]
    uds: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();

    // Construct our ABCI application.
    let service = KVStore::default();

    // Split it into components.
    let (consensus, mempool, snapshot, info) = split::service(service, 1);

    // Hand those components to the ABCI server, but customize request behavior
    // for each category -- for instance, apply load-shedding only to mempool
    // and info requests, but not to consensus requests.
    let server_builder = Server::builder()
        .consensus(consensus)
        .snapshot(snapshot)
        .mempool(
            ServiceBuilder::new()
                .load_shed()
                .buffer(10)
                .service(mempool),
        )
        .info(
            ServiceBuilder::new()
                .load_shed()
                .buffer(100)
                .rate_limit(50, std::time::Duration::from_secs(1))
                .service(info),
        );

    let server = server_builder.finish().unwrap();

    if let Some(uds_path) = opt.uds {
        server.listen_unix(uds_path).await.unwrap();
    } else {
        server
            .listen_tcp(format!("{}:{}", opt.host, opt.port))
            .await
            .unwrap();
    }
}