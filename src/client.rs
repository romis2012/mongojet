use crate::database::CoreDatabase;
use crate::error::MongoError;
use crate::options::{CoreDatabaseOptions, CoreSessionOptions};
use crate::runtime::spawn;
use crate::session::CoreSession;
use log::debug;
use mongodb::options::{ClientOptions, DatabaseOptions, SessionOptions};
use mongodb::Client;
use pyo3::prelude::*;

#[pyclass]
pub struct CoreClient {
    pub client: Client,
    #[pyo3(get)]
    pub default_database_name: Option<String>,
}

#[pyfunction]
pub async fn core_create_client(url: String) -> PyResult<CoreClient> {
    let fut = async move {
        let options = ClientOptions::parse(url)
            .await
            .map_err(|e| MongoError::from(e))?;

        let default_database_name = options.default_database.clone();

        debug!("create_client options: {:?}", options);

        let client = Client::with_options(options).map_err(|e| MongoError::from(e))?;

        Ok(CoreClient {
            client,
            default_database_name,
        })
    };
    spawn(fut).await?
}

#[pymethods]
impl CoreClient {
    pub fn get_default_database(&self) -> PyResult<Option<CoreDatabase>> {
        match self.client.default_database() {
            Some(db) => Ok(Some(CoreDatabase::new(db))),
            None => Ok(None),
        }
    }

    pub fn get_database(&self, name: String) -> PyResult<CoreDatabase> {
        let db = self.client.database(name.as_str());
        Ok(CoreDatabase::new(db))
    }

    pub fn get_database_with_options(
        &self,
        name: String,
        options: CoreDatabaseOptions,
    ) -> PyResult<CoreDatabase> {
        let opts: DatabaseOptions = options.into();

        debug!("get_database_with_options options: {:?}", opts);

        let db = self.client.database_with_options(name.as_str(), opts);
        Ok(CoreDatabase::new(db))
    }

    #[pyo3(signature = (options=None))]
    pub async fn start_session(
        &self,
        options: Option<CoreSessionOptions>,
    ) -> PyResult<CoreSession> {
        let c = self.client.clone();
        let options: Option<SessionOptions> = options.map(Into::into);

        debug!("Client.start_session, options: {:?}", options);

        let fut = async move {
            let s = c
                .start_session()
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?;
            Ok(CoreSession::new(s))
        };
        spawn(fut).await?
    }

    pub async fn shutdown(&self) -> PyResult<()> {
        let client = self.client.clone();

        debug!("Client.shutdown");

        let fut = async move {
            client.shutdown().await;
            Ok(())
        };

        spawn(fut).await?
    }

    pub async fn shutdown_immediate(&self) -> PyResult<()> {
        let client = self.client.clone();

        debug!("Client.shutdown_immediate");

        let fut = async move {
            client.shutdown().immediate(true).await;
            Ok(())
        };

        spawn(fut).await?
    }
}
