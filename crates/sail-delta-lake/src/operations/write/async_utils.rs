// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/operations/write/async_utils.rs>

//! Async Sharable Buffer for async writer

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::TryFuture;
use tokio::io::AsyncWrite;
use tokio::sync::RwLock as TokioRwLock;

/// An in-memory buffer that allows for shared ownership and interior mutability.
/// The underlying buffer is wrapped in an `Arc` and `RwLock`, so cloning the instance
/// allows multiple owners to have access to the same underlying buffer.
#[derive(Debug, Default, Clone)]
pub struct AsyncShareableBuffer {
    buffer: Arc<TokioRwLock<Vec<u8>>>,
}

impl AsyncShareableBuffer {
    /// Consumes this instance and returns the underlying buffer.
    /// Returns `None` if there are other references to the instance.
    pub async fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.buffer)
            .ok()
            .map(|lock| lock.into_inner())
    }

    #[allow(dead_code)]
    pub async fn to_vec(&self) -> Vec<u8> {
        let inner = self.buffer.read().await;
        inner.clone()
    }

    pub async fn len(&self) -> usize {
        let inner = self.buffer.read().await;
        inner.len()
    }

    #[allow(dead_code)]
    pub async fn is_empty(&self) -> bool {
        let inner = self.buffer.read().await;
        inner.is_empty()
    }
}

impl AsyncWrite for AsyncShareableBuffer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.clone();
        let buf = buf.to_vec();

        let fut = async move {
            let mut buffer = this.buffer.write().await;
            buffer.extend_from_slice(&buf);
            Ok(buf.len())
        };

        tokio::pin!(fut);
        fut.try_poll(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
