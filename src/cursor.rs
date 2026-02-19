use std::ops::DerefMut;
use std::sync::Arc;

use bson::{doc, RawArray, RawArrayBuf, RawDocumentBuf};
use futures::stream::StreamExt;
use futures::TryStreamExt;
use mongodb::raw_batch_cursor::{RawBatchCursor, SessionRawBatchCursor};
use mongodb::{ClientSession, Cursor, SessionCursor};
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::document::{CoreRawArray, CoreRawDocument};
use crate::error::MongoError;
use crate::runtime::spawn;

#[pyclass]
pub struct CoreCursor {
    pub cursor: Arc<Mutex<Cursor<RawDocumentBuf>>>,
}

impl CoreCursor {
    pub fn new(cursor: Cursor<RawDocumentBuf>) -> Self {
        Self {
            cursor: Arc::new(Mutex::new(cursor)),
        }
    }
}

#[pymethods]
impl CoreCursor {
    pub async fn next(&mut self) -> PyResult<CoreRawDocument> {
        let cursor = Arc::clone(&self.cursor);
        let fut = async move {
            let result: Option<CoreRawDocument> = cursor
                .lock()
                .await
                .try_next()
                .await
                .map_err(MongoError::from)?
                .map(Into::into);

            if let Some(doc) = result {
                return Ok(doc);
            }

            Err(PyStopAsyncIteration::new_err("StopAsyncIteration"))
        };

        spawn(fut).await?
    }

    pub async fn collect(&mut self) -> PyResult<Vec<CoreRawDocument>> {
        let cursor = Arc::clone(&self.cursor);

        let fut = async move {
            let mut result: Vec<CoreRawDocument> = Vec::new();
            let mut cursor = cursor.lock().await;

            while let Some(doc) = cursor.try_next().await.map_err(MongoError::from)? {
                result.push(doc.into());
            }

            Ok(result)
        };

        spawn(fut).await?
    }

    pub async fn next_batch(&mut self, batch_size: u64) -> PyResult<Vec<CoreRawDocument>> {
        let cursor = Arc::clone(&self.cursor);
        let fut = async move {
            let mut result = Vec::with_capacity(batch_size as usize);
            let mut cursor = cursor.lock().await;

            for _ in 0..batch_size {
                let ok = cursor.advance().await.map_err(MongoError::from)?;

                if !ok {
                    break;
                }

                let doc: CoreRawDocument = cursor
                    .deserialize_current()
                    .map_err(MongoError::from)?
                    .into();

                result.push(doc);
            }

            Ok(result)
        };

        spawn(fut).await?
    }
}

#[pyclass]
pub struct CoreSessionCursor {
    pub cursor: Arc<Mutex<SessionCursor<RawDocumentBuf>>>,
    pub session: Arc<Mutex<ClientSession>>,
}

impl CoreSessionCursor {
    pub fn new(cursor: SessionCursor<RawDocumentBuf>, session: Arc<Mutex<ClientSession>>) -> Self {
        Self {
            cursor: Arc::new(Mutex::new(cursor)),
            session,
        }
    }
}

#[pymethods]
impl CoreSessionCursor {
    pub async fn next(&mut self) -> PyResult<CoreRawDocument> {
        let cursor = Arc::clone(&self.cursor);
        let session = Arc::clone(&self.session);

        let fut = async move {
            let result: Option<CoreRawDocument> = cursor
                .lock()
                .await
                .next(session.lock().await.deref_mut())
                .await
                .transpose()
                .map_err(MongoError::from)?
                .map(Into::into);

            if let Some(doc) = result {
                return Ok(doc);
            }

            Err(PyStopAsyncIteration::new_err("StopAsyncIteration"))
        };

        spawn(fut).await?
    }

    pub async fn next_batch(&mut self, batch_size: u64) -> PyResult<Vec<CoreRawDocument>> {
        let cursor = Arc::clone(&self.cursor);
        let session = Arc::clone(&self.session);

        let fut = async move {
            let mut result: Vec<CoreRawDocument> = Vec::with_capacity(batch_size as usize);

            let mut cursor = cursor.lock().await;
            let mut session = session.lock().await;

            for _ in 0..batch_size {
                if let Some(doc) = cursor
                    .next(session.deref_mut())
                    .await
                    .transpose()
                    .map_err(MongoError::from)?
                {
                    result.push(doc.into());
                } else {
                    break;
                }
            }

            Ok(result)
        };

        spawn(fut).await?
    }

    pub async fn collect(&mut self) -> PyResult<Vec<CoreRawDocument>> {
        let cursor = Arc::clone(&self.cursor);
        let session = Arc::clone(&self.session);

        let fut = async move {
            let mut result: Vec<CoreRawDocument> = Vec::new();

            let mut cursor = cursor.lock().await;
            let mut session = session.lock().await;

            while let Some(doc) = cursor
                .next(session.deref_mut())
                .await
                .transpose()
                .map_err(MongoError::from)?
            {
                result.push(doc.into());
            }

            Ok(result)
        };

        spawn(fut).await?
    }
}

//
#[pyclass]
pub struct CoreBatchCursor {
    pub cursor: Arc<Mutex<RawBatchCursor>>,
}

impl CoreBatchCursor {
    pub fn new(cursor: RawBatchCursor) -> Self {
        Self {
            cursor: Arc::new(Mutex::new(cursor)),
        }
    }
}

#[pymethods]
impl CoreBatchCursor {
    pub async fn next_batch(&mut self) -> PyResult<CoreRawArray> {
        let cursor = Arc::clone(&self.cursor);

        let fut = async move {
            let mut cursor = cursor.lock().await;

            if let Some(batch) = cursor.next().await {
                let batch = batch.map_err(MongoError::from)?;
                let data: &RawArray = batch.doc_slices().map_err(MongoError::from)?;
                return Ok(data.to_owned().into());
            }
            Ok(RawArrayBuf::new().into())
        };
        spawn(fut).await?
    }

    pub async fn collect(&mut self) -> PyResult<CoreRawArray> {
        let cursor = Arc::clone(&self.cursor);

        let fut = async move {
            let mut arr: RawArrayBuf = RawArrayBuf::new();
            let mut cursor = cursor.lock().await;

            while let Some(batch) = cursor.next().await {
                let batch = batch.map_err(MongoError::from)?;
                let data: &RawArray = batch.doc_slices().map_err(MongoError::from)?;
                for doc_result in data {
                    let doc = doc_result.map_err(MongoError::from)?;
                    arr.push(doc);
                }
            }
            Ok(arr.into())
        };
        spawn(fut).await?
    }
}

#[pyclass]
pub struct CoreSessionBatchCursor {
    pub cursor: Arc<Mutex<SessionRawBatchCursor>>,
    pub session: Arc<Mutex<ClientSession>>,
}

impl CoreSessionBatchCursor {
    pub fn new(cursor: SessionRawBatchCursor, session: Arc<Mutex<ClientSession>>) -> Self {
        Self {
            cursor: Arc::new(Mutex::new(cursor)),
            session,
        }
    }
}

#[pymethods]
impl CoreSessionBatchCursor {
    pub async fn next_batch(&mut self) -> PyResult<CoreRawArray> {
        let cursor = Arc::clone(&self.cursor);
        let session = Arc::clone(&self.session);

        let fut = async move {
            let mut arr: RawArrayBuf = RawArrayBuf::new();

            let mut cursor = cursor.lock().await;
            let mut session = session.lock().await;

            if let Some(batch) = cursor.stream(session.deref_mut()).next().await {
                let batch = batch.map_err(MongoError::from)?;
                let data: &RawArray = batch.doc_slices().map_err(MongoError::from)?;
                for doc_result in data {
                    let doc = doc_result.map_err(MongoError::from)?;
                    arr.push(doc);
                }
            }
            Ok(arr.into())
        };
        spawn(fut).await?
    }

    pub async fn collect(&mut self) -> PyResult<CoreRawArray> {
        let cursor = Arc::clone(&self.cursor);
        let session = Arc::clone(&self.session);

        let fut = async move {
            let mut arr: RawArrayBuf = RawArrayBuf::new();

            let mut cursor = cursor.lock().await;
            let mut session = session.lock().await;

            while let Some(batch) = cursor.stream(session.deref_mut()).next().await {
                let batch = batch.map_err(MongoError::from)?;
                let data: &RawArray = batch.doc_slices().map_err(MongoError::from)?;
                for doc_result in data {
                    let doc = doc_result.map_err(MongoError::from)?;
                    arr.push(doc);
                }
            }
            Ok(arr.into())
        };
        spawn(fut).await?
    }
}
