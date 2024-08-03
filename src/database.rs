use crate::collection::CoreCollection;
use crate::cursor::{CoreCursor, CoreSessionCursor};
use crate::document::{CoreDocument, CorePipeline};
use crate::error::MongoError;
use crate::gridfs::CoreGridFsBucket;
use crate::options::{
    CoreAggregateOptions, CoreCollectionOptions, CoreCreateCollectionOptions,
    CoreDropDatabaseOptions, CoreGridFsBucketOptions, CoreListCollectionsOptions,
    CoreRunCommandOptions,
};
use crate::result::{
    CoreCollectionSpecification, ReadConcernResult, ReadPreferenceResult, WriteConcernResult,
};
use crate::runtime::spawn;
use crate::session::CoreSession;
use bson::{Document, RawDocumentBuf};
use futures::TryStreamExt;
use log::debug;
use mongodb::action::Action;
use mongodb::options::{
    AggregateOptions, CollectionOptions, CreateCollectionOptions, DropDatabaseOptions,
    GridFsBucketOptions, ListCollectionsOptions, SelectionCriteria,
};
use mongodb::results::CollectionSpecification;
use mongodb::Database;
use pyo3::prelude::*;
use std::ops::DerefMut;
use std::sync::Arc;

#[pyclass]
pub struct CoreDatabase {
    pub db: Database,
    #[pyo3(get)]
    pub name: String,
}

impl CoreDatabase {
    pub fn new(db: Database) -> Self {
        let name = db.name().to_string();
        Self { db, name }
    }
}

#[pymethods]
impl CoreDatabase {
    pub fn get_collection(&self, name: String) -> PyResult<CoreCollection> {
        let col = self.db.collection::<RawDocumentBuf>(name.as_str());

        debug!("{:?}.get_collection", self.name);

        Ok(CoreCollection::new(col))
    }

    pub fn get_collection_with_options(
        &self,
        name: String,
        options: CoreCollectionOptions,
    ) -> PyResult<CoreCollection> {
        let opts: CollectionOptions = options.into();

        debug!(
            "{:?}.get_collection_with_options options: {:?}",
            self.name, opts
        );

        let col = self
            .db
            .collection_with_options::<RawDocumentBuf>(name.as_str(), opts);

        Ok(CoreCollection::new(col))
    }

    #[pyo3(signature = (name, options=None))]
    pub async fn create_collection(
        &self,
        name: String,
        options: Option<CoreCreateCollectionOptions>,
    ) -> PyResult<()> {
        let db = self.db.clone();

        let options: Option<CreateCollectionOptions> = options.map(|o| o.into());

        debug!("{:?}.create_collection, options: {:?}", self.name, options);

        let fut = async move {
            db.create_collection(name)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(())
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, name, options=None))]
    pub async fn create_collection_with_session(
        &self,
        session: Py<CoreSession>,
        name: String,
        options: Option<CoreCreateCollectionOptions>,
    ) -> PyResult<()> {
        let db = self.db.clone();

        let options: Option<CreateCollectionOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.create_collection_with_session, options: {:?}",
            self.name, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            db.create_collection(name)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(())
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter=None, options=None))]
    pub async fn list_collections(
        &self,
        filter: Option<CoreDocument>,
        options: Option<CoreListCollectionsOptions>,
    ) -> PyResult<Vec<CoreCollectionSpecification>> {
        let db = self.db.clone();

        let filter: Option<Document> = filter.map(Into::into);
        let options: Option<ListCollectionsOptions> = options.map(Into::into);

        debug!(
            "{:?}.list_collections, filter: {:?}, options: {:?}",
            self.name, filter, options
        );

        let fut = async move {
            let docs: Vec<CoreCollectionSpecification> = db
                .list_collections()
                .with_options(options)
                .filter(filter.unwrap_or_default())
                .await
                .map_err(|e| MongoError::from(e))?
                .try_collect::<Vec<CollectionSpecification>>()
                .await
                .map_err(|e| MongoError::from(e))?
                .into_iter()
                .map(CoreCollectionSpecification::from)
                .collect();

            Ok(docs)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter=None, options=None))]
    pub async fn list_collections_with_session(
        &self,
        session: Py<CoreSession>,
        filter: Option<CoreDocument>,
        options: Option<CoreListCollectionsOptions>,
    ) -> PyResult<Vec<CoreCollectionSpecification>> {
        let db = self.db.clone();

        let filter: Option<Document> = filter.map(Into::into);
        let options: Option<ListCollectionsOptions> = options.map(Into::into);

        debug!(
            "{:?}.list_collections_with_session, filter: {:?}, options: {:?}",
            self.name, filter, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let mut session = session.lock().await;

            let docs: Vec<CoreCollectionSpecification> = db
                .list_collections()
                .with_options(options)
                .filter(filter.unwrap_or_default())
                .session(session.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .stream(&mut session.deref_mut())
                .try_collect::<Vec<CollectionSpecification>>()
                .await
                .map_err(|e| MongoError::from(e))?
                .into_iter()
                .map(CoreCollectionSpecification::from)
                .collect();

            Ok(docs)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (command, options=None))]
    pub async fn run_command(
        &self,
        command: CoreDocument,
        options: Option<CoreRunCommandOptions>,
    ) -> PyResult<CoreDocument> {
        let db = self.db.clone();

        let command: Document = command.into();
        let selection_criteria: Option<SelectionCriteria> = options
            .and_then(|o| o.read_preference)
            .map(|p| SelectionCriteria::ReadPreference(p));

        debug!("{:?}.run_command, command: {:?}", self.name, command);

        let fut = async move {
            let result: CoreDocument = db
                .run_command(command)
                .optional(selection_criteria, |cmd, sc| cmd.selection_criteria(sc))
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, command, options=None))]
    pub async fn run_command_with_session(
        &self,
        session: Py<CoreSession>,
        command: CoreDocument,
        options: Option<CoreRunCommandOptions>,
    ) -> PyResult<CoreDocument> {
        let db = self.db.clone();

        let command: Document = command.into();
        let selection_criteria: Option<SelectionCriteria> = options
            .and_then(|o| o.read_preference)
            .map(|p| SelectionCriteria::ReadPreference(p));

        debug!(
            "{:?}.run_command_with_session, command: {:?}",
            self.name, command
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let mut session = session.lock().await;

            let mut command = db.run_command(command).session(session.deref_mut());

            if let Some(sc) = selection_criteria {
                command = command.selection_criteria(sc);
            }

            let result: CoreDocument = command.await.map_err(|e| MongoError::from(e))?.into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (pipeline, options=None))]
    pub async fn aggregate(
        &self,
        pipeline: CorePipeline,
        options: Option<CoreAggregateOptions>,
    ) -> PyResult<CoreCursor> {
        let db = self.db.clone();

        let options: Option<AggregateOptions> = options.map(Into::into);

        debug!(
            "{:?}.aggregate, pipeline: {:?}, options: {:?}",
            self.name, pipeline, options
        );

        let fut = async move {
            let cur = db
                .aggregate(pipeline)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(CoreCursor::new(cur.with_type()))
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, pipeline, options=None))]
    pub async fn aggregate_with_session(
        &self,
        session: Py<CoreSession>,
        pipeline: CorePipeline,
        options: Option<CoreAggregateOptions>,
    ) -> PyResult<CoreSessionCursor> {
        let db = self.db.clone();

        let options: Option<AggregateOptions> = options.map(Into::into);

        debug!(
            "{:?}.aggregate_with_session, pipeline: {:?}, options: {:?}",
            self.name, pipeline, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let cur = db
                .aggregate(pipeline)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(CoreSessionCursor::new(
                cur.with_type(),
                Arc::clone(&session),
            ))
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (options=None))]
    pub fn gridfs_bucket(
        &self,
        options: Option<CoreGridFsBucketOptions>,
    ) -> PyResult<CoreGridFsBucket> {
        let options: Option<GridFsBucketOptions> = options.map(Into::into);

        debug!("{:?}.gridfs_bucket options: {:?}", self.name, options);

        let bucket = self.db.gridfs_bucket(options);

        Ok(CoreGridFsBucket::new(bucket))
    }

    #[pyo3(signature = (options=None))]
    pub async fn drop(&self, options: Option<CoreDropDatabaseOptions>) -> PyResult<()> {
        let db = self.db.clone();

        let options: Option<DropDatabaseOptions> = options.map(|o| o.into());

        debug!("{:?}.drop, options: {:?}", self.name, options);

        let fut = async move {
            db.drop()
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?;
            Ok(())
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, options=None))]
    pub async fn drop_with_session(
        &self,
        session: Py<CoreSession>,
        options: Option<CoreDropDatabaseOptions>,
    ) -> PyResult<()> {
        let db = self.db.clone();

        let options: Option<DropDatabaseOptions> = options.map(|o| o.into());

        debug!("{:?}.drop_with_session, options: {:?}", self.name, options);

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            db.drop()
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(())
        };

        spawn(fut).await?
    }

    pub fn read_preference(&self) -> Option<ReadPreferenceResult> {
        let sc = self.db.selection_criteria().cloned();
        match sc {
            Some(SelectionCriteria::ReadPreference(p)) => Some(p.into()),
            _ => None,
        }
    }

    pub fn write_concern(&self) -> Option<WriteConcernResult> {
        self.db.write_concern().cloned().map(|wc| wc.into())
    }

    pub fn read_concern(&self) -> Option<ReadConcernResult> {
        self.db.read_concern().cloned().map(|wc| wc.into())
    }
}
