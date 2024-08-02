use std::time::Duration;

use bson::{Bson, Document};
use mongodb::options::{
    AggregateOptions, ChangeStreamPreAndPostImages, ClusteredIndex, Collation, CollectionOptions,
    CommitQuorum, CountOptions, CreateCollectionOptions, CreateIndexOptions, CursorType,
    DatabaseOptions, DeleteOptions, DistinctOptions, DropCollectionOptions, DropDatabaseOptions,
    DropIndexOptions, EstimatedDocumentCountOptions, FindOneAndDeleteOptions,
    FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOneOptions, FindOptions,
    GridFsBucketOptions, Hint, IndexOptionDefaults, InsertManyOptions, InsertOneOptions,
    ListCollectionsOptions, ListIndexesOptions, ReadConcern, ReadPreference, ReplaceOptions,
    ReturnDocument, SelectionCriteria, SessionOptions, TimeseriesOptions, TransactionOptions,
    UpdateOptions, ValidationAction, ValidationLevel, WriteConcern,
};
use mongodb::IndexModel;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use serde::{Deserialize, Serialize};

use crate::conv::{from_py_object, into_py_object};

#[derive(Clone, Debug, Deserialize)]
pub struct CoreDatabaseOptions {
    pub read_concern: Option<ReadConcern>,
    pub write_concern: Option<WriteConcern>,
    pub read_preference: Option<ReadPreference>,
}

from_py_object!(CoreDatabaseOptions);

impl Into<DatabaseOptions> for CoreDatabaseOptions {
    fn into(self) -> DatabaseOptions {
        let selection_criteria: Option<SelectionCriteria> = self
            .read_preference
            .map(|p| SelectionCriteria::ReadPreference(p));

        DatabaseOptions::builder()
            .selection_criteria(selection_criteria)
            .read_concern(self.read_concern)
            .write_concern(self.write_concern)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreCollectionOptions {
    pub read_concern: Option<ReadConcern>,
    pub write_concern: Option<WriteConcern>,
    pub read_preference: Option<ReadPreference>,
}

from_py_object!(CoreCollectionOptions);

impl Into<CollectionOptions> for CoreCollectionOptions {
    fn into(self) -> CollectionOptions {
        let selection_criteria: Option<SelectionCriteria> = self
            .read_preference
            .map(|p| SelectionCriteria::ReadPreference(p));

        CollectionOptions::builder()
            .selection_criteria(selection_criteria)
            .read_concern(self.read_concern)
            .write_concern(self.write_concern)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreFindOptions {
    pub sort: Option<Document>,
    pub projection: Option<Document>,
    pub skip: Option<u64>,
    pub limit: Option<i64>,
    pub cursor_type: Option<CursorType>,
    pub no_cursor_timeout: Option<bool>,
    pub allow_partial_results: Option<bool>,
    pub batch_size: Option<u32>,
    pub max_time_ms: Option<u64>,
    pub allow_disk_use: Option<bool>,
    pub max: Option<Document>,
    pub min: Option<Document>,
    pub hint: Option<Hint>,
    pub collation: Option<Collation>,
    pub comment: Option<Bson>,
    pub max_await_time_ms: Option<u64>,
    pub max_scan: Option<u64>,
    pub read_concern: Option<ReadConcern>,
    pub read_preference: Option<ReadPreference>,
    pub return_key: Option<bool>,
    pub show_record_id: Option<bool>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
}

from_py_object!(CoreFindOptions);

impl Into<FindOptions> for CoreFindOptions {
    fn into(self) -> FindOptions {
        let selection_criteria: Option<SelectionCriteria> = self
            .read_preference
            .map(|p| SelectionCriteria::ReadPreference(p));

        FindOptions::builder()
            .sort(self.sort)
            .projection(self.projection)
            .skip(self.skip)
            .limit(self.limit)
            .cursor_type(self.cursor_type)
            .no_cursor_timeout(self.no_cursor_timeout)
            .allow_partial_results(self.allow_partial_results)
            .batch_size(self.batch_size)
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .allow_disk_use(self.allow_disk_use)
            .max(self.max)
            .min(self.min)
            .hint(self.hint)
            .collation(self.collation)
            .comment(self.comment)
            .max_await_time(self.max_await_time_ms.map(Duration::from_millis))
            .max_scan(self.max_scan)
            .read_concern(self.read_concern)
            .selection_criteria(selection_criteria)
            .return_key(self.return_key)
            .show_record_id(self.show_record_id)
            .let_vars(self.let_vars)
            .build()
    }
}

// the same as CoreOneOptions excluding:
// limit, cursor_type, no_cursor_timeout, batch_size, allow_disk_use, max_await_time_ms
#[derive(Clone, Debug, Deserialize)]
pub struct CoreFindOneOptions {
    pub sort: Option<Document>,
    pub projection: Option<Document>,
    pub skip: Option<u64>,
    pub allow_partial_results: Option<bool>,
    pub max_time_ms: Option<u64>,
    pub max: Option<Document>,
    pub min: Option<Document>,
    pub hint: Option<Hint>,
    pub collation: Option<Collation>,
    pub comment: Option<Bson>,
    pub max_scan: Option<u64>,
    pub read_concern: Option<ReadConcern>,
    pub read_preference: Option<ReadPreference>,
    pub return_key: Option<bool>,
    pub show_record_id: Option<bool>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
}

from_py_object!(CoreFindOneOptions);

impl Into<FindOneOptions> for CoreFindOneOptions {
    fn into(self) -> FindOneOptions {
        let selection_criteria: Option<SelectionCriteria> = self
            .read_preference
            .map(|p| SelectionCriteria::ReadPreference(p));

        FindOneOptions::builder()
            .sort(self.sort)
            .projection(self.projection)
            .skip(self.skip)
            .allow_partial_results(self.allow_partial_results)
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .max(self.max)
            .min(self.min)
            .hint(self.hint)
            .collation(self.collation)
            .comment(self.comment)
            .max_scan(self.max_scan)
            .read_concern(self.read_concern)
            .selection_criteria(selection_criteria)
            .return_key(self.return_key)
            .show_record_id(self.show_record_id)
            .let_vars(self.let_vars)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreFindOneAndUpdateOptions {
    pub projection: Option<Document>,
    pub sort: Option<Document>,
    pub upsert: Option<bool>,
    pub return_document: Option<ReturnDocument>, //after, before
    pub array_filters: Option<Vec<Document>>,
    pub hint: Option<Hint>,
    pub collation: Option<Collation>,
    pub bypass_document_validation: Option<bool>,
    pub max_time_ms: Option<u64>,
    pub write_concern: Option<WriteConcern>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreFindOneAndUpdateOptions);

impl Into<FindOneAndUpdateOptions> for CoreFindOneAndUpdateOptions {
    fn into(self) -> FindOneAndUpdateOptions {
        FindOneAndUpdateOptions::builder()
            .projection(self.projection)
            .sort(self.sort)
            .upsert(self.upsert)
            .return_document(self.return_document)
            .array_filters(self.array_filters)
            .hint(self.hint)
            .collation(self.collation)
            .bypass_document_validation(self.bypass_document_validation)
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .write_concern(self.write_concern)
            .let_vars(self.let_vars)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreFindOneAndReplaceOptions {
    pub projection: Option<Document>,
    pub sort: Option<Document>,
    pub upsert: Option<bool>,
    pub return_document: Option<ReturnDocument>,
    pub hint: Option<Hint>,
    pub collation: Option<Collation>,
    pub bypass_document_validation: Option<bool>,
    pub max_time_ms: Option<u64>,
    pub write_concern: Option<WriteConcern>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreFindOneAndReplaceOptions);

impl Into<FindOneAndReplaceOptions> for CoreFindOneAndReplaceOptions {
    fn into(self) -> FindOneAndReplaceOptions {
        FindOneAndReplaceOptions::builder()
            .projection(self.projection)
            .sort(self.sort)
            .upsert(self.upsert)
            .return_document(self.return_document)
            .hint(self.hint)
            .collation(self.collation)
            .bypass_document_validation(self.bypass_document_validation)
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .write_concern(self.write_concern)
            .let_vars(self.let_vars)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreFindOneAndDeleteOptions {
    pub projection: Option<Document>,
    pub sort: Option<Document>,
    pub hint: Option<Hint>,
    pub collation: Option<Collation>,
    pub max_time_ms: Option<u64>,
    pub write_concern: Option<WriteConcern>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreFindOneAndDeleteOptions);

impl Into<FindOneAndDeleteOptions> for CoreFindOneAndDeleteOptions {
    fn into(self) -> FindOneAndDeleteOptions {
        FindOneAndDeleteOptions::builder()
            .projection(self.projection)
            .sort(self.sort)
            .hint(self.hint)
            .collation(self.collation)
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .write_concern(self.write_concern)
            .let_vars(self.let_vars)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreAggregateOptions {
    pub bypass_document_validation: Option<bool>,
    pub batch_size: Option<u32>,
    pub max_time_ms: Option<u64>,
    pub allow_disk_use: Option<bool>,
    pub hint: Option<Hint>,
    pub collation: Option<Collation>,
    pub comment: Option<Bson>,
    pub max_await_time_ms: Option<u64>,
    pub read_concern: Option<ReadConcern>,
    pub read_preference: Option<ReadPreference>,
    pub write_concern: Option<WriteConcern>, //+
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
}

from_py_object!(CoreAggregateOptions);

impl Into<AggregateOptions> for CoreAggregateOptions {
    fn into(self) -> AggregateOptions {
        let selection_criteria: Option<SelectionCriteria> = self
            .read_preference
            .map(|p| SelectionCriteria::ReadPreference(p));

        AggregateOptions::builder()
            .bypass_document_validation(self.bypass_document_validation)
            .batch_size(self.batch_size)
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .allow_disk_use(self.allow_disk_use)
            .hint(self.hint)
            .collation(self.collation)
            .comment(self.comment)
            .max_await_time(self.max_await_time_ms.map(Duration::from_millis))
            .read_concern(self.read_concern)
            .write_concern(self.write_concern)
            .selection_criteria(selection_criteria)
            .let_vars(self.let_vars)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreUpdateOptions {
    pub upsert: Option<bool>,
    pub bypass_document_validation: Option<bool>,
    pub collation: Option<Collation>,
    pub array_filters: Option<Vec<Document>>,
    pub hint: Option<Hint>,
    pub write_concern: Option<WriteConcern>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreUpdateOptions);

impl Into<UpdateOptions> for CoreUpdateOptions {
    fn into(self) -> UpdateOptions {
        UpdateOptions::builder()
            .upsert(self.upsert)
            .bypass_document_validation(self.bypass_document_validation)
            .collation(self.collation)
            .array_filters(self.array_filters)
            .hint(self.hint)
            .write_concern(self.write_concern)
            .let_vars(self.let_vars)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreReplaceOptions {
    pub upsert: Option<bool>,
    pub bypass_document_validation: Option<bool>,
    pub collation: Option<Collation>,
    pub hint: Option<Hint>,
    pub write_concern: Option<WriteConcern>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreReplaceOptions);

impl Into<ReplaceOptions> for CoreReplaceOptions {
    fn into(self) -> ReplaceOptions {
        ReplaceOptions::builder()
            .upsert(self.upsert)
            .bypass_document_validation(self.bypass_document_validation)
            .collation(self.collation)
            .hint(self.hint)
            .write_concern(self.write_concern)
            .let_vars(self.let_vars)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreInsertOneOptions {
    pub bypass_document_validation: Option<bool>,
    pub write_concern: Option<WriteConcern>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreInsertOneOptions);

impl Into<InsertOneOptions> for CoreInsertOneOptions {
    fn into(self) -> InsertOneOptions {
        InsertOneOptions::builder()
            .bypass_document_validation(self.bypass_document_validation)
            .write_concern(self.write_concern)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreInsertManyOptions {
    pub ordered: Option<bool>,
    pub bypass_document_validation: Option<bool>,
    pub write_concern: Option<WriteConcern>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreInsertManyOptions);

impl Into<InsertManyOptions> for CoreInsertManyOptions {
    fn into(self) -> InsertManyOptions {
        InsertManyOptions::builder()
            .ordered(self.ordered)
            .bypass_document_validation(self.bypass_document_validation)
            .write_concern(self.write_concern)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreDeleteOptions {
    pub collation: Option<Collation>,
    pub hint: Option<Hint>,
    pub write_concern: Option<WriteConcern>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreDeleteOptions);

impl Into<DeleteOptions> for CoreDeleteOptions {
    fn into(self) -> DeleteOptions {
        DeleteOptions::builder()
            .collation(self.collation)
            .hint(self.hint)
            .write_concern(self.write_concern)
            .let_vars(self.let_vars)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreCountOptions {
    pub skip: Option<u64>,
    pub limit: Option<u64>,
    pub max_time_ms: Option<u64>,
    pub hint: Option<Hint>,
    pub collation: Option<Collation>,
    pub read_preference: Option<ReadPreference>,
    pub read_concern: Option<ReadConcern>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreCountOptions);

impl Into<CountOptions> for CoreCountOptions {
    fn into(self) -> CountOptions {
        let selection_criteria: Option<SelectionCriteria> = self
            .read_preference
            .map(|p| SelectionCriteria::ReadPreference(p));

        CountOptions::builder()
            .skip(self.skip)
            .limit(self.limit)
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .hint(self.hint)
            .collation(self.collation)
            .selection_criteria(selection_criteria)
            .read_concern(self.read_concern)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreEstimatedCountOptions {
    pub max_time_ms: Option<u64>,
    pub read_preference: Option<ReadPreference>,
    pub read_concern: Option<ReadConcern>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreEstimatedCountOptions);

impl Into<EstimatedDocumentCountOptions> for CoreEstimatedCountOptions {
    fn into(self) -> EstimatedDocumentCountOptions {
        let selection_criteria: Option<SelectionCriteria> = self
            .read_preference
            .map(|p| SelectionCriteria::ReadPreference(p));

        EstimatedDocumentCountOptions::builder()
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .selection_criteria(selection_criteria)
            .read_concern(self.read_concern)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreDistinctOptions {
    pub max_time_ms: Option<u64>,
    pub read_preference: Option<ReadPreference>,
    pub read_concern: Option<ReadConcern>,
    pub collation: Option<Collation>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreDistinctOptions);

impl Into<DistinctOptions> for CoreDistinctOptions {
    fn into(self) -> DistinctOptions {
        let selection_criteria: Option<SelectionCriteria> = self
            .read_preference
            .map(|p| SelectionCriteria::ReadPreference(p));

        DistinctOptions::builder()
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .selection_criteria(selection_criteria)
            .read_concern(self.read_concern)
            .collation(self.collation)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CoreIndexModel(IndexModel);

from_py_object!(CoreIndexModel);
into_py_object!(CoreIndexModel);

impl From<IndexModel> for CoreIndexModel {
    fn from(value: IndexModel) -> Self {
        CoreIndexModel(value)
    }
}

impl Into<IndexModel> for CoreIndexModel {
    fn into(self) -> IndexModel {
        self.0
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoreCreateIndexOptions {
    #[serde(rename = "maxTimeMS")]
    pub max_time_ms: Option<u64>,
    pub comment: Option<Bson>,
    pub write_concern: Option<WriteConcern>,
    pub commit_quorum: Option<CommitQuorum>,
}

from_py_object!(CoreCreateIndexOptions);

impl Into<CreateIndexOptions> for CoreCreateIndexOptions {
    fn into(self) -> CreateIndexOptions {
        CreateIndexOptions::builder()
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .comment(self.comment)
            .write_concern(self.write_concern)
            .commit_quorum(self.commit_quorum)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoreDropIndexOptions {
    #[serde(rename = "maxTimeMS")]
    pub max_time_ms: Option<u64>,
    pub write_concern: Option<WriteConcern>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreDropIndexOptions);

impl Into<DropIndexOptions> for CoreDropIndexOptions {
    fn into(self) -> DropIndexOptions {
        DropIndexOptions::builder()
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .comment(self.comment)
            .write_concern(self.write_concern)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoreListIndexesOptions {
    #[serde(rename = "maxTimeMS")]
    pub max_time_ms: Option<u64>,
    pub batch_size: Option<u32>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreListIndexesOptions);

impl Into<ListIndexesOptions> for CoreListIndexesOptions {
    fn into(self) -> ListIndexesOptions {
        ListIndexesOptions::builder()
            .max_time(self.max_time_ms.map(Duration::from_millis))
            .batch_size(self.batch_size)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreDropCollectionOptions {
    pub write_concern: Option<WriteConcern>,
}

from_py_object!(CoreDropCollectionOptions);

impl Into<DropCollectionOptions> for CoreDropCollectionOptions {
    fn into(self) -> DropCollectionOptions {
        DropCollectionOptions::builder()
            .write_concern(self.write_concern)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreTransactionOptions {
    pub read_concern: Option<ReadConcern>,
    pub write_concern: Option<WriteConcern>,
    pub read_preference: Option<ReadPreference>,
    pub max_commit_time_ms: Option<u64>,
}

from_py_object!(CoreTransactionOptions);

impl Into<TransactionOptions> for CoreTransactionOptions {
    fn into(self) -> TransactionOptions {
        let selection_criteria: Option<SelectionCriteria> = self
            .read_preference
            .map(|p| SelectionCriteria::ReadPreference(p));

        TransactionOptions::builder()
            .read_concern(self.read_concern)
            .write_concern(self.write_concern)
            .selection_criteria(selection_criteria)
            .max_commit_time(self.max_commit_time_ms.map(Duration::from_millis))
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreSessionOptions {
    pub causal_consistency: Option<bool>,
    pub default_transaction_options: Option<CoreTransactionOptions>,
    pub snapshot: Option<bool>,
}

from_py_object!(CoreSessionOptions);

impl Into<SessionOptions> for CoreSessionOptions {
    fn into(self) -> SessionOptions {
        SessionOptions::builder()
            .causal_consistency(self.causal_consistency)
            .default_transaction_options(self.default_transaction_options.map(Into::into))
            .snapshot(self.snapshot)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoreCreateCollectionOptions {
    pub capped: Option<bool>,
    pub size: Option<u64>,
    pub max: Option<u64>,
    pub storage_engine: Option<Document>,
    pub validator: Option<Document>,
    pub validation_level: Option<ValidationLevel>,
    pub validation_action: Option<ValidationAction>,
    pub view_on: Option<String>,
    pub pipeline: Option<Vec<Document>>,
    pub collation: Option<Collation>,
    pub write_concern: Option<WriteConcern>,
    pub index_option_defaults: Option<IndexOptionDefaults>,
    pub timeseries: Option<TimeseriesOptions>,
    pub expire_after_seconds: Option<u64>,
    pub change_stream_pre_and_post_images: Option<ChangeStreamPreAndPostImages>,
    pub clustered_index: Option<ClusteredIndex>,
    pub comment: Option<Bson>,
    // /// Map of encrypted fields for the created collection.
    // #[cfg(feature = "in-use-encryption-unstable")]
    // pub encrypted_fields: Option<Document>,
}

from_py_object!(CoreCreateCollectionOptions);

impl Into<CreateCollectionOptions> for CoreCreateCollectionOptions {
    fn into(self) -> CreateCollectionOptions {
        CreateCollectionOptions::builder()
            .capped(self.capped)
            .size(self.size)
            .max(self.max)
            .storage_engine(self.storage_engine)
            .validator(self.validator)
            .validation_level(self.validation_level)
            .validation_action(self.validation_action)
            .view_on(self.view_on)
            .pipeline(self.pipeline)
            .collation(self.collation)
            .write_concern(self.write_concern)
            .index_option_defaults(self.index_option_defaults)
            .timeseries(self.timeseries)
            .expire_after_seconds(self.expire_after_seconds.map(Duration::from_secs))
            .change_stream_pre_and_post_images(self.change_stream_pre_and_post_images)
            .clustered_index(self.clustered_index)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoreListCollectionsOptions {
    pub batch_size: Option<u32>,
    pub comment: Option<Bson>,
}

from_py_object!(CoreListCollectionsOptions);

impl Into<ListCollectionsOptions> for CoreListCollectionsOptions {
    fn into(self) -> ListCollectionsOptions {
        ListCollectionsOptions::builder()
            .batch_size(self.batch_size)
            .comment(self.comment)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreRunCommandOptions {
    pub read_preference: Option<ReadPreference>,
}

from_py_object!(CoreRunCommandOptions);

#[derive(Clone, Debug, Deserialize)]
pub struct CoreGridFsBucketOptions {
    pub bucket_name: Option<String>,
    pub chunk_size_bytes: Option<u32>,
    pub write_concern: Option<WriteConcern>,
    pub read_concern: Option<ReadConcern>,
    pub read_preference: Option<ReadPreference>,
}

from_py_object!(CoreGridFsBucketOptions);

impl Into<GridFsBucketOptions> for CoreGridFsBucketOptions {
    fn into(self) -> GridFsBucketOptions {
        let selection_criteria: Option<SelectionCriteria> = self
            .read_preference
            .map(|p| SelectionCriteria::ReadPreference(p));

        GridFsBucketOptions::builder()
            .bucket_name(self.bucket_name)
            .chunk_size_bytes(self.chunk_size_bytes)
            .read_concern(self.read_concern)
            .write_concern(self.write_concern)
            .selection_criteria(selection_criteria)
            .build()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CoreGridFsPutOptions {
    pub file_id: Option<Bson>,
    pub filename: Option<String>,
}
from_py_object!(CoreGridFsPutOptions);

#[derive(Clone, Debug, Deserialize)]
pub struct CoreGridFsGetByIdOptions {
    pub file_id: Bson,
}
from_py_object!(CoreGridFsGetByIdOptions);

#[derive(Clone, Debug, Deserialize)]
pub struct CoreGridFsGetByNameOptions {
    pub filename: String,
}
from_py_object!(CoreGridFsGetByNameOptions);

#[derive(Clone, Debug, Deserialize)]
pub struct CoreDropDatabaseOptions {
    pub write_concern: Option<WriteConcern>,
}

impl Into<DropDatabaseOptions> for CoreDropDatabaseOptions {
    fn into(self) -> DropDatabaseOptions {
        DropDatabaseOptions::builder()
            .write_concern(self.write_concern)
            .build()
    }
}

from_py_object!(CoreDropDatabaseOptions);
