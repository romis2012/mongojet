use bson::{Bson, Document};
use mongodb::options::{CreateCollectionOptions, ReadConcern, ReadPreference, WriteConcern};
use mongodb::results::{
    CollectionSpecification, CollectionSpecificationInfo, CollectionType, CreateIndexResult,
    CreateIndexesResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use serde::Serialize;

use crate::conv::into_py_object;

#[derive(Clone, Debug, Serialize)]
pub struct CoreDistinctResult {
    pub values: Vec<Bson>,
}

into_py_object!(CoreDistinctResult);

impl From<Vec<Bson>> for CoreDistinctResult {
    fn from(value: Vec<Bson>) -> Self {
        Self { values: value }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CoreUpdateResult {
    pub matched_count: u64,
    pub modified_count: u64,
    pub upserted_id: Option<Bson>,
}

into_py_object!(CoreUpdateResult);

impl From<UpdateResult> for CoreUpdateResult {
    fn from(v: UpdateResult) -> Self {
        Self {
            matched_count: v.matched_count,
            modified_count: v.modified_count,
            upserted_id: v.upserted_id,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CoreInsertOneResult {
    pub inserted_id: Bson,
}

into_py_object!(CoreInsertOneResult);

impl From<InsertOneResult> for CoreInsertOneResult {
    fn from(v: InsertOneResult) -> Self {
        Self {
            inserted_id: v.inserted_id,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CoreInsertManyResult {
    pub inserted_ids: Vec<Bson>,
}

into_py_object!(CoreInsertManyResult);

impl From<InsertManyResult> for CoreInsertManyResult {
    fn from(v: InsertManyResult) -> Self {
        Self {
            inserted_ids: v.inserted_ids.values().cloned().collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CoreDeleteResult {
    pub deleted_count: u64,
}

into_py_object!(CoreDeleteResult);

impl From<DeleteResult> for CoreDeleteResult {
    fn from(v: DeleteResult) -> Self {
        Self {
            deleted_count: v.deleted_count,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CoreCreateIndexResult {
    pub index_name: String,
}

into_py_object!(CoreCreateIndexResult);

impl From<CreateIndexResult> for CoreCreateIndexResult {
    fn from(v: CreateIndexResult) -> Self {
        Self {
            index_name: v.index_name,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CoreCreateIndexesResult {
    pub index_names: Vec<String>,
}

into_py_object!(CoreCreateIndexesResult);

impl From<CreateIndexesResult> for CoreCreateIndexesResult {
    fn from(v: CreateIndexesResult) -> Self {
        Self {
            index_names: v.index_names,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CoreCollectionSpecification {
    pub name: String,
    #[serde(rename = "type")]
    pub collection_type: CollectionType,
    pub options: CreateCollectionOptions,
    pub info: CollectionSpecificationInfo,
    pub id_index: Option<Document>,
}

into_py_object!(CoreCollectionSpecification);

impl From<CollectionSpecification> for CoreCollectionSpecification {
    fn from(v: CollectionSpecification) -> Self {
        Self {
            name: v.name,
            collection_type: v.collection_type,
            options: v.options,
            info: v.info,
            id_index: v.id_index,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ReadPreferenceResult(ReadPreference);

into_py_object!(ReadPreferenceResult);

impl From<ReadPreference> for ReadPreferenceResult {
    fn from(value: ReadPreference) -> Self {
        ReadPreferenceResult(value)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct WriteConcernResult(WriteConcern);

into_py_object!(WriteConcernResult);

impl From<WriteConcern> for WriteConcernResult {
    fn from(value: WriteConcern) -> Self {
        WriteConcernResult(value)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ReadConcernResult(ReadConcern);

into_py_object!(ReadConcernResult);

impl From<ReadConcern> for ReadConcernResult {
    fn from(value: ReadConcern) -> Self {
        ReadConcernResult(value)
    }
}
