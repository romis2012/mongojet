use std::ops::DerefMut;
use std::sync::Arc;

use bson::{Document, RawDocumentBuf};
use futures::TryStreamExt;
use log::debug;
use mongodb::options::{
    AggregateOptions, CountOptions, CreateIndexOptions, DeleteOptions, DistinctOptions,
    DropCollectionOptions, DropIndexOptions, EstimatedDocumentCountOptions,
    FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOneOptions,
    FindOptions, InsertManyOptions, InsertOneOptions, ListIndexesOptions, ReplaceOptions,
    SelectionCriteria, UpdateModifications, UpdateOptions,
};
use mongodb::{Collection, IndexModel};
use pyo3::prelude::*;

use crate::cursor::{CoreCursor, CoreSessionCursor};
use crate::document::{CoreCompoundDocument, CoreDocument, CorePipeline, CoreRawDocument};
use crate::result::{
    CoreCreateIndexResult, CoreCreateIndexesResult, CoreDeleteResult, CoreDistinctResult,
    CoreInsertManyResult, CoreInsertOneResult, CoreUpdateResult, ReadConcernResult,
    ReadPreferenceResult, WriteConcernResult,
};

use crate::error::MongoError;
use crate::options::{
    CoreAggregateOptions, CoreCountOptions, CoreCreateIndexOptions, CoreDeleteOptions,
    CoreDistinctOptions, CoreDropCollectionOptions, CoreDropIndexOptions,
    CoreEstimatedCountOptions, CoreFindOneAndDeleteOptions, CoreFindOneAndReplaceOptions,
    CoreFindOneAndUpdateOptions, CoreFindOneOptions, CoreFindOptions, CoreIndexModel,
    CoreInsertManyOptions, CoreInsertOneOptions, CoreListIndexesOptions, CoreReplaceOptions,
    CoreUpdateOptions,
};
use crate::runtime::spawn;
use crate::session::CoreSession;

#[pyclass]
pub struct CoreCollection {
    collection: Collection<RawDocumentBuf>,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub full_name: String,
}

impl CoreCollection {
    pub fn new(collection: Collection<RawDocumentBuf>) -> Self {
        let name = collection.name().to_string();
        let full_name = collection.namespace().to_string();
        Self {
            collection,
            name,
            full_name,
        }
    }
}

#[pymethods]
impl CoreCollection {
    #[pyo3(signature = (filter=None, options=None))]
    pub async fn find_one(
        &self,
        filter: Option<CoreDocument>,
        options: Option<CoreFindOneOptions>,
    ) -> PyResult<Option<CoreRawDocument>> {
        let collection = self.collection.clone();

        let filter: Option<Document> = filter.map(Into::into);
        let options: Option<FindOneOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_one, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let fut = async move {
            let result: Option<CoreRawDocument> = collection
                .find_one(filter.unwrap_or_default())
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .map(Into::into);

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter=None, options=None))]
    pub async fn find_one_with_session(
        &self,
        session: Py<CoreSession>,
        filter: Option<CoreDocument>,
        options: Option<CoreFindOneOptions>,
    ) -> PyResult<Option<CoreRawDocument>> {
        let collection = self.collection.clone();

        let filter: Option<Document> = filter.map(Into::into);
        let options: Option<FindOneOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_one_with_session, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: Option<CoreRawDocument> = collection
                .find_one(filter.unwrap_or_default())
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .map(Into::into);

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter, update, options=None))]
    pub async fn find_one_and_update(
        &self,
        filter: CoreDocument,
        update: CoreCompoundDocument,
        options: Option<CoreFindOneAndUpdateOptions>,
    ) -> PyResult<Option<CoreRawDocument>> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let update: UpdateModifications = update.into();
        let options: Option<FindOneAndUpdateOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_one_and_update, filter: {:?}, update: {:?}, options: {:?}",
            self.full_name, filter, update, options
        );

        let fut = async move {
            let result: Option<CoreRawDocument> = collection
                .find_one_and_update(filter, update)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .map(Into::into);

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter, update, options=None))]
    pub async fn find_one_and_update_with_session(
        &self,
        session: Py<CoreSession>,
        filter: CoreDocument,
        update: CoreCompoundDocument,
        options: Option<CoreFindOneAndUpdateOptions>,
    ) -> PyResult<Option<CoreRawDocument>> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let update: UpdateModifications = update.into();
        let options: Option<FindOneAndUpdateOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_one_and_update, filter: {:?}, update: {:?}, options: {:?}",
            self.full_name, filter, update, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: Option<CoreRawDocument> = collection
                .find_one_and_update(filter, update)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .map(Into::into);

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter, replacement, options=None))]
    pub async fn find_one_and_replace(
        &self,
        filter: CoreDocument,
        replacement: CoreRawDocument,
        options: Option<CoreFindOneAndReplaceOptions>,
    ) -> PyResult<Option<CoreRawDocument>> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let replacement: RawDocumentBuf = replacement.into();
        let options: Option<FindOneAndReplaceOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_one_and_replace, filter: {:?}, replacement: {:?}, options: {:?}",
            self.full_name, filter, replacement, options
        );

        let fut = async move {
            let result: Option<CoreRawDocument> = collection
                .find_one_and_replace(filter, replacement)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .map(Into::into);

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter, replacement, options=None))]
    pub async fn find_one_and_replace_with_session(
        &self,
        session: Py<CoreSession>,
        filter: CoreDocument,
        replacement: CoreRawDocument,
        options: Option<CoreFindOneAndReplaceOptions>,
    ) -> PyResult<Option<CoreRawDocument>> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let replacement: RawDocumentBuf = replacement.into();
        let options: Option<FindOneAndReplaceOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_one_and_replace, filter: {:?}, replacement: {:?}, options: {:?}",
            self.full_name, filter, replacement, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: Option<CoreRawDocument> = collection
                .find_one_and_replace(filter, replacement)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .map(Into::into);

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter, options=None))]
    pub async fn find_one_and_delete(
        &self,
        filter: CoreDocument,
        options: Option<CoreFindOneAndDeleteOptions>,
    ) -> PyResult<Option<CoreRawDocument>> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let options: Option<FindOneAndDeleteOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_one_and_delete, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );
        let fut = async move {
            let result: Option<CoreRawDocument> = collection
                .find_one_and_delete(filter)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .map(Into::into);

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter, options=None))]
    pub async fn find_one_and_delete_with_session(
        &self,
        session: Py<CoreSession>,
        filter: CoreDocument,
        options: Option<CoreFindOneAndDeleteOptions>,
    ) -> PyResult<Option<CoreRawDocument>> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let options: Option<FindOneAndDeleteOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_one_and_delete, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: Option<CoreRawDocument> = collection
                .find_one_and_delete(filter)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .map(Into::into);

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter=None, options=None))]
    pub async fn find(
        &self,
        filter: Option<CoreDocument>,
        options: Option<CoreFindOptions>,
    ) -> PyResult<CoreCursor> {
        let collection = self.collection.clone();

        let filter: Option<Document> = filter.map(Into::into);
        let options: Option<FindOptions> = options.map(Into::into);

        debug!(
            "{:?}.find, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let fut = async move {
            let cur = collection
                .find(filter.unwrap_or_default())
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(CoreCursor::new(cur))
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter=None, options=None))]
    pub async fn find_with_session(
        &self,
        session: Py<CoreSession>,
        filter: Option<CoreDocument>,
        options: Option<CoreFindOptions>,
    ) -> PyResult<CoreSessionCursor> {
        let collection = self.collection.clone();

        let filter: Option<Document> = filter.map(Into::into);
        let options: Option<FindOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_with_session, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let cur = collection
                .find(filter.unwrap_or_default())
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(CoreSessionCursor::new(cur, Arc::clone(&session)))
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter=None, options=None))]
    pub async fn find_many(
        &self,
        filter: Option<CoreDocument>,
        options: Option<CoreFindOptions>,
    ) -> PyResult<Vec<CoreRawDocument>> {
        let collection = self.collection.clone();

        let filter: Option<Document> = filter.map(Into::into);
        let options: Option<FindOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_many, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let fut = async move {
            let docs: Vec<CoreRawDocument> = collection
                .find(filter.unwrap_or_default())
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| MongoError::from(e))?
                .into_iter()
                .map(CoreRawDocument::from)
                .collect();

            Ok(docs)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter=None, options=None))]
    pub async fn find_many_with_session(
        &self,
        session: Py<CoreSession>,
        filter: Option<CoreDocument>,
        options: Option<CoreFindOptions>,
    ) -> PyResult<Vec<CoreRawDocument>> {
        let collection = self.collection.clone();

        let filter: Option<Document> = filter.map(Into::into);
        let options: Option<FindOptions> = options.map(Into::into);

        debug!(
            "{:?}.find_many_with_session, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let mut session = session.lock().await;

            let docs: Vec<CoreRawDocument> = collection
                .find(filter.unwrap_or_default())
                .with_options(options)
                .session(session.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .stream(&mut session.deref_mut())
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| MongoError::from(e))?
                .into_iter()
                .map(CoreRawDocument::from)
                .collect();

            Ok(docs)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (pipeline, options=None))]
    pub async fn aggregate(
        &self,
        pipeline: CorePipeline,
        options: Option<CoreAggregateOptions>,
    ) -> PyResult<CoreCursor> {
        let collection = self.collection.clone();

        let options: Option<AggregateOptions> = options.map(Into::into);

        debug!(
            "{:?}.aggregate, pipeline: {:?}, options: {:?}",
            self.full_name, pipeline, options
        );

        let fut = async move {
            let cur = collection
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
        let collection = self.collection.clone();

        let options: Option<AggregateOptions> = options.map(Into::into);

        debug!(
            "{:?}.aggregate, pipeline: {:?}, options: {:?}",
            self.full_name, pipeline, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let cur = collection
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

    #[pyo3(signature = (field_name, filter=None, options=None))]
    pub async fn distinct(
        &self,
        field_name: String,
        filter: Option<CoreDocument>,
        options: Option<CoreDistinctOptions>,
    ) -> PyResult<CoreDistinctResult> {
        let collection = self.collection.clone();

        let filter: Option<Document> = filter.map(Into::into);
        let options: Option<DistinctOptions> = options.map(Into::into);

        debug!(
            "{:?}.distinct, field_name: {:?}, filter: {:?}, options: {:?}",
            self.full_name, field_name, filter, options
        );

        let fut = async move {
            let result: CoreDistinctResult = collection
                .distinct(field_name, filter.unwrap_or_default())
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, field_name, filter=None, options=None))]
    pub async fn distinct_with_session(
        &self,
        session: Py<CoreSession>,
        field_name: String,
        filter: Option<CoreDocument>,
        options: Option<CoreDistinctOptions>,
    ) -> PyResult<CoreDistinctResult> {
        let collection = self.collection.clone();

        let filter: Option<Document> = filter.map(Into::into);
        let options: Option<DistinctOptions> = options.map(Into::into);

        debug!(
            "{:?}.distinct_with_session, field_name: {:?}, filter: {:?}, options: {:?}",
            self.full_name, field_name, filter, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: CoreDistinctResult = collection
                .distinct(field_name, filter.unwrap_or_default())
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter, update, options=None))]
    pub async fn update_one(
        &self,
        filter: CoreDocument,
        update: CoreCompoundDocument,
        options: Option<CoreUpdateOptions>,
    ) -> PyResult<CoreUpdateResult> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let update: UpdateModifications = update.into();
        let options: Option<UpdateOptions> = options.map(|o| o.into());

        // dbg!("update options: {:?}", options.clone());

        debug!(
            "{:?}.update_one, filter: {:?}, update: {:?}, options: {:?}",
            self.full_name, filter, update, options
        );

        let fut = async move {
            let result: CoreUpdateResult = collection
                .update_one(filter, update)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter, update, options=None))]
    pub async fn update_one_with_session(
        &self,
        session: Py<CoreSession>,
        filter: CoreDocument,
        update: CoreCompoundDocument,
        options: Option<CoreUpdateOptions>,
    ) -> PyResult<CoreUpdateResult> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let update: UpdateModifications = update.into();
        let options: Option<UpdateOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.update_one, filter: {:?}, update: {:?}, options: {:?}",
            self.full_name, filter, update, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: CoreUpdateResult = collection
                .update_one(filter, update)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter, update, options=None))]
    pub async fn update_many(
        &self,
        filter: CoreDocument,
        update: CoreCompoundDocument,
        options: Option<CoreUpdateOptions>,
    ) -> PyResult<CoreUpdateResult> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let update: UpdateModifications = update.into();
        let options: Option<UpdateOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.update_many, filter: {:?}, update: {:?}, options: {:?}",
            self.full_name, filter, update, options
        );

        let fut = async move {
            let result: CoreUpdateResult = collection
                .update_many(filter, update)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter, update, options=None))]
    pub async fn update_many_with_session(
        &self,
        session: Py<CoreSession>,
        filter: CoreDocument,
        update: CoreCompoundDocument,
        options: Option<CoreUpdateOptions>,
    ) -> PyResult<CoreUpdateResult> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let update: UpdateModifications = update.into();
        let options: Option<UpdateOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.update_many, filter: {:?}, update: {:?}, options: {:?}",
            self.full_name, filter, update, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: CoreUpdateResult = collection
                .update_many(filter, update)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (document, options=None))]
    pub async fn insert_one(
        &self,
        document: CoreRawDocument,
        options: Option<CoreInsertOneOptions>,
    ) -> PyResult<CoreInsertOneResult> {
        let collection = self.collection.clone();

        let document: RawDocumentBuf = document.into();
        let options: Option<InsertOneOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.insert_one, document: {:?}, options: {:?}",
            self.full_name, document, options
        );

        let fut = async move {
            let result: CoreInsertOneResult = collection
                .insert_one(document)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, document, options=None))]
    pub async fn insert_one_with_session(
        &self,
        session: Py<CoreSession>,
        document: CoreRawDocument,
        options: Option<CoreInsertOneOptions>,
    ) -> PyResult<CoreInsertOneResult> {
        let collection = self.collection.clone();

        let document: RawDocumentBuf = document.into();
        let options: Option<InsertOneOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.insert_one_with_session, document: {:?}, options: {:?}",
            self.full_name, document, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: CoreInsertOneResult = collection
                .insert_one(document)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (documents, options=None))]
    pub async fn insert_many(
        &self,
        documents: Vec<CoreRawDocument>,
        options: Option<CoreInsertManyOptions>,
    ) -> PyResult<CoreInsertManyResult> {
        let collection = self.collection.clone();

        let documents: Vec<RawDocumentBuf> = documents.into_iter().map(|d| d.into()).collect();
        let options: Option<InsertManyOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.insert_many, documents: {:?}, options: {:?}",
            self.full_name, documents, options
        );

        let fut = async move {
            let result: CoreInsertManyResult = collection
                .insert_many(documents)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, documents, options=None))]
    pub async fn insert_many_with_session(
        &self,
        session: Py<CoreSession>,
        documents: Vec<CoreRawDocument>,
        options: Option<CoreInsertManyOptions>,
    ) -> PyResult<CoreInsertManyResult> {
        let collection = self.collection.clone();

        let documents: Vec<RawDocumentBuf> = documents.into_iter().map(|d| d.into()).collect();
        let options: Option<InsertManyOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.insert_many, documents: {:?}, options: {:?}",
            self.full_name, documents, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: CoreInsertManyResult = collection
                .insert_many(documents)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter, replacement, options=None))]
    pub async fn replace_one(
        &self,
        filter: CoreDocument,
        replacement: CoreRawDocument,
        options: Option<CoreReplaceOptions>,
    ) -> PyResult<CoreUpdateResult> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let replacement: RawDocumentBuf = replacement.into();
        let options: Option<ReplaceOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.replace_one, filter: {:?}, replacement: {:?}, options: {:?}",
            self.full_name, filter, replacement, options
        );

        let fut = async move {
            let result: CoreUpdateResult = collection
                .replace_one(filter, replacement)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter, replacement, options=None))]
    pub async fn replace_one_with_session(
        &self,
        session: Py<CoreSession>,
        filter: CoreDocument,
        replacement: CoreRawDocument,
        options: Option<CoreReplaceOptions>,
    ) -> PyResult<CoreUpdateResult> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let replacement: RawDocumentBuf = replacement.into();
        let options: Option<ReplaceOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.replace_one, filter: {:?}, replacement: {:?}, options: {:?}",
            self.full_name, filter, replacement, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: CoreUpdateResult = collection
                .replace_one(filter, replacement)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter, options=None))]
    pub async fn delete_one(
        &self,
        filter: CoreDocument,
        options: Option<CoreDeleteOptions>,
    ) -> PyResult<CoreDeleteResult> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let options: Option<DeleteOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.delete_one, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let fut = async move {
            let result: CoreDeleteResult = collection
                .delete_one(filter)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter, options=None))]
    pub async fn delete_one_with_session(
        &self,
        session: Py<CoreSession>,
        filter: CoreDocument,
        options: Option<CoreDeleteOptions>,
    ) -> PyResult<CoreDeleteResult> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let options: Option<DeleteOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.delete_one, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: CoreDeleteResult = collection
                .delete_one(filter)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter, options=None))]
    pub async fn delete_many(
        &self,
        filter: CoreDocument,
        options: Option<CoreDeleteOptions>,
    ) -> PyResult<CoreDeleteResult> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let options: Option<DeleteOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.delete_many, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let fut = async move {
            let result: CoreDeleteResult = collection
                .delete_many(filter)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter, options=None))]
    pub async fn delete_many_with_session(
        &self,
        session: Py<CoreSession>,
        filter: CoreDocument,
        options: Option<CoreDeleteOptions>,
    ) -> PyResult<CoreDeleteResult> {
        let collection = self.collection.clone();

        let filter: Document = filter.into();
        let options: Option<DeleteOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.delete_many, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: CoreDeleteResult = collection
                .delete_many(filter)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (filter=None, options=None))]
    pub async fn count_documents(
        &self,
        filter: Option<CoreDocument>,
        options: Option<CoreCountOptions>,
    ) -> PyResult<u64> {
        let collection = self.collection.clone();

        let filter: Option<Document> = filter.map(|o| o.into());
        let options: Option<CountOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.count_documents, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let fut = async move {
            let result = collection
                .count_documents(filter.unwrap_or_default())
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, filter=None, options=None))]
    pub async fn count_documents_with_session(
        &self,
        session: Py<CoreSession>,
        filter: Option<CoreDocument>,
        options: Option<CoreCountOptions>,
    ) -> PyResult<u64> {
        let collection = self.collection.clone();

        let filter: Option<Document> = filter.map(|o| o.into());
        let options: Option<CountOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.count_documents, filter: {:?}, options: {:?}",
            self.full_name, filter, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result = collection
                .count_documents(filter.unwrap_or_default())
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (options=None))]
    pub async fn estimated_document_count(
        &self,
        options: Option<CoreEstimatedCountOptions>,
    ) -> PyResult<u64> {
        let collection = self.collection.clone();

        let options: Option<EstimatedDocumentCountOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.estimated_document_count, options: {:?}",
            self.full_name, options
        );

        let fut = async move {
            let result = collection
                .estimated_document_count()
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (model, options=None))]
    pub async fn create_index(
        &self,
        model: CoreIndexModel,
        options: Option<CoreCreateIndexOptions>,
    ) -> PyResult<CoreCreateIndexResult> {
        let collection = self.collection.clone();

        let model: IndexModel = model.into();
        let options: Option<CreateIndexOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.create_index, model: {:?}, options: {:?}",
            self.full_name, model, options
        );

        let fut = async move {
            let result: CoreCreateIndexResult = collection
                .create_index(model)
                .with_options(options)
                .await
                // .map_err(Into::<MongoError>::into)?
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, model, options=None))]
    pub async fn create_index_with_session(
        &self,
        session: Py<CoreSession>,
        model: CoreIndexModel,
        options: Option<CoreCreateIndexOptions>,
    ) -> PyResult<CoreCreateIndexResult> {
        let collection = self.collection.clone();

        let model: IndexModel = model.into();
        let options: Option<CreateIndexOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.create_index, model: {:?}, options: {:?}",
            self.full_name, model, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: CoreCreateIndexResult = collection
                .create_index(model)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (model, options=None))]
    pub async fn create_indexes(
        &self,
        model: Vec<CoreIndexModel>,
        options: Option<CoreCreateIndexOptions>,
    ) -> PyResult<CoreCreateIndexesResult> {
        let collection = self.collection.clone();

        let model: Vec<IndexModel> = model.into_iter().map(|m| m.into()).collect();
        let options: Option<CreateIndexOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.create_indexes, model: {:?}, options: {:?}",
            self.full_name, model, options
        );

        let fut = async move {
            let result: CoreCreateIndexesResult = collection
                .create_indexes(model)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, model, options=None))]
    pub async fn create_indexes_with_session(
        &self,
        session: Py<CoreSession>,
        model: Vec<CoreIndexModel>,
        options: Option<CoreCreateIndexOptions>,
    ) -> PyResult<CoreCreateIndexesResult> {
        let collection = self.collection.clone();

        let model: Vec<IndexModel> = model.into_iter().map(|m| m.into()).collect();
        let options: Option<CreateIndexOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.create_indexes, model: {:?}, options: {:?}",
            self.full_name, model, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let result: CoreCreateIndexesResult = collection
                .create_indexes(model)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .into();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (name, options=None))]
    pub async fn drop_index(
        &self,
        name: String,
        options: Option<CoreDropIndexOptions>,
    ) -> PyResult<()> {
        let collection = self.collection.clone();

        let options: Option<DropIndexOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.drop_index, name: {:?}, options: {:?}",
            self.full_name, name, options
        );

        let fut = async move {
            collection
                .drop_index(name)
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(())
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, name, options=None))]
    pub async fn drop_index_with_session(
        &self,
        session: Py<CoreSession>,
        name: String,
        options: Option<CoreDropIndexOptions>,
    ) -> PyResult<()> {
        let collection = self.collection.clone();

        let options: Option<DropIndexOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.drop_index_with_session, name: {:?}, options: {:?}",
            self.full_name, name, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            collection
                .drop_index(name)
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(())
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (options=None))]
    pub async fn drop_indexes(&self, options: Option<CoreDropIndexOptions>) -> PyResult<()> {
        let collection = self.collection.clone();

        let options: Option<DropIndexOptions> = options.map(|o| o.into());

        debug!("{:?}.drop_indexes, options: {:?}", self.full_name, options);

        let fut = async move {
            collection
                .drop_indexes()
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(())
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, options=None))]
    pub async fn drop_indexes_with_session(
        &self,
        session: Py<CoreSession>,
        options: Option<CoreDropIndexOptions>,
    ) -> PyResult<()> {
        let collection = self.collection.clone();

        let options: Option<DropIndexOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.drop_indexes_with_session, options: {:?}",
            self.full_name, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            collection
                .drop_indexes()
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(())
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (options=None))]
    pub async fn list_indexes(
        &self,
        options: Option<CoreListIndexesOptions>,
    ) -> PyResult<Vec<CoreIndexModel>> {
        let collection = self.collection.clone();

        let options: Option<ListIndexesOptions> = options.map(|o| o.into());

        debug!("{:?}.list_indexes, options: {:?}", self.full_name, options);

        let fut = async move {
            let result: Vec<CoreIndexModel> = collection
                .list_indexes()
                .with_options(options)
                .await
                .map_err(|e| MongoError::from(e))?
                .try_collect::<Vec<IndexModel>>()
                .await
                .map_err(|e| MongoError::from(e))?
                .into_iter()
                .map(CoreIndexModel::from)
                .collect();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (session, options=None))]
    pub async fn list_indexes_with_session(
        &self,
        session: Py<CoreSession>,
        options: Option<CoreListIndexesOptions>,
    ) -> PyResult<Vec<CoreIndexModel>> {
        let collection = self.collection.clone();

        let options: Option<ListIndexesOptions> = options.map(|o| o.into());

        debug!(
            "{:?}.list_indexes_with_session, options: {:?}",
            self.full_name, options
        );

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            let mut session = session.lock().await;
            let result: Vec<CoreIndexModel> = collection
                .list_indexes()
                .with_options(options)
                .session(session.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?
                .stream(&mut session.deref_mut())
                .try_collect::<Vec<IndexModel>>()
                .await
                .map_err(|e| MongoError::from(e))?
                .into_iter()
                .map(CoreIndexModel::from)
                .collect();

            Ok(result)
        };

        spawn(fut).await?
    }

    #[pyo3(signature = (options=None))]
    pub async fn drop(&self, options: Option<CoreDropCollectionOptions>) -> PyResult<()> {
        let collection = self.collection.clone();

        let options: Option<DropCollectionOptions> = options.map(|o| o.into());

        debug!("{:?}.drop, options: {:?}", self.full_name, options);

        let fut = async move {
            collection
                .drop()
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
        options: Option<CoreDropCollectionOptions>,
    ) -> PyResult<()> {
        let collection = self.collection.clone();

        let options: Option<DropCollectionOptions> = options.map(|o| o.into());

        debug!("{:?}.drop, options: {:?}", self.full_name, options);

        let session = Python::with_gil(|py| session.borrow(py).session.clone());

        let fut = async move {
            collection
                .drop()
                .with_options(options)
                .session(session.lock().await.deref_mut())
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(())
        };

        spawn(fut).await?
    }

    pub fn read_preference(&self) -> Option<ReadPreferenceResult> {
        let sc = self.collection.selection_criteria().cloned();
        match sc {
            Some(SelectionCriteria::ReadPreference(p)) => Some(p.into()),
            _ => None,
        }
    }

    pub fn write_concern(&self) -> Option<WriteConcernResult> {
        self.collection.write_concern().cloned().map(|wc| wc.into())
    }

    pub fn read_concern(&self) -> Option<ReadConcernResult> {
        self.collection.read_concern().cloned().map(|wc| wc.into())
    }
}
