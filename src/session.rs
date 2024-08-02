use log::debug;
use std::sync::Arc;

use crate::error::MongoError;
use crate::options::CoreTransactionOptions;
use crate::runtime::spawn;
use mongodb::options::TransactionOptions;
use mongodb::ClientSession;
use pyo3::prelude::*;
use tokio::sync::Mutex;

#[pyclass]
pub struct CoreSession {
    pub session: Arc<Mutex<ClientSession>>,
}

impl CoreSession {
    pub fn new(session: ClientSession) -> Self {
        CoreSession {
            session: Arc::new(Mutex::new(session)),
        }
    }
}

#[pymethods]
impl CoreSession {
    #[pyo3(signature = (options=None))]
    pub async fn start_transaction(
        &mut self,
        options: Option<CoreTransactionOptions>,
    ) -> PyResult<()> {
        let options: Option<TransactionOptions> = options.map(|o| o.into());

        debug!("session.start_transaction, options: {:?}", options);

        let s = Arc::clone(&self.session);
        let fut = async move {
            s.lock()
                .await
                .start_transaction()
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?;
            Ok(())
        };

        spawn(fut).await?
    }

    pub async fn commit_transaction(&mut self) -> PyResult<()> {
        debug!("session.commit_transaction");

        let s = Arc::clone(&self.session);
        let fut = async move {
            s.lock()
                .await
                .commit_transaction()
                .await
                .map_err(|e| MongoError::from(e))?;
            Ok(())
        };
        spawn(fut).await?
    }

    pub async fn abort_transaction(&mut self) -> PyResult<()> {
        debug!("session.abort_transaction");

        let s = Arc::clone(&self.session);
        let fut = async move {
            s.lock()
                .await
                .abort_transaction()
                .await
                .map_err(|e| MongoError::from(e))?;
            Ok(())
        };

        spawn(fut).await?
    }
}
