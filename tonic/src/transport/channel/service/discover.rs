use super::super::{Connection, Endpoint};

use http::Uri;
use hyper::rt;
use std::{
    hash::Hash,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc::Receiver;

use tokio_stream::Stream;
use tower::discover::Change;
use tower_service::Service;

type DiscoverResult<K, S, E> = Result<Change<K, S>, E>;

pub(crate) struct DynamicServiceStream<K: Hash + Eq + Clone, B> {
    changes: Receiver<Change<K, Endpoint>>,
    connector_builder: B,
}

impl<K: Hash + Eq + Clone, B> DynamicServiceStream<K, B> {
    pub(crate) fn new(changes: Receiver<Change<K, Endpoint>>, connector_builder: B) -> Self {
        Self {
            changes,
            connector_builder,
        }
    }
}

impl<K, B, C> Stream for DynamicServiceStream<K, B>
where
    K: Hash + Eq + Clone,
    B: Fn(&Endpoint) -> C,
    C: Service<Uri> + Clone + Send + 'static,
    C::Future: Send,
    C::Response: rt::Read + rt::Write + Unpin + Send + 'static,
    crate::Error: From<C::Error> + Send,
{
    type Item = DiscoverResult<K, Connection, crate::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let c = &mut self.changes;
        match Pin::new(&mut *c).poll_recv(cx) {
            Poll::Pending | Poll::Ready(None) => Poll::Pending,
            Poll::Ready(Some(change)) => match change {
                Change::Insert(k, endpoint) => {
                    let connection = Connection::lazy(
                        endpoint.connector((self.connector_builder)(&endpoint)),
                        endpoint,
                    );
                    let change = Ok(Change::Insert(k, connection));
                    Poll::Ready(Some(change))
                }
                Change::Remove(k) => Poll::Ready(Some(Ok(Change::Remove(k)))),
            },
        }
    }
}

impl<K: Hash + Eq + Clone, C> Unpin for DynamicServiceStream<K, C> {}
