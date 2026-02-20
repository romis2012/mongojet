use std::ops::DerefMut;
use std::sync::Arc;

use bson::{doc, RawArray, RawArrayBuf};
use futures::stream::StreamExt;
use mongodb::raw_batch_cursor::{RawBatchCursor, SessionRawBatchCursor};
use mongodb::ClientSession;
use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::document::CoreRawArray;
use crate::error::MongoError;
use crate::runtime::spawn;

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
