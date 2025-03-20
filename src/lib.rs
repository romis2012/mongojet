mod client;
mod collection;
mod conv;
mod cursor;
mod database;
mod document;
mod error;
mod gridfs;
mod options;
mod result;
mod runtime;
mod session;

use pyo3::prelude::*;

use crate::error::{
    BsonDeserializationError, BsonSerializationError, ConfigurationError, ConnectionFailure,
    FileExists, GridFSError, NoFile, ServerSelectionError,
};
use client::{core_create_client, CoreClient};
use collection::CoreCollection;
use cursor::CoreCursor;
use database::CoreDatabase;
use error::{DuplicateKeyError, OperationFailure, PyMongoError, WriteConcernError, WriteError};

#[rustfmt::skip]
#[pymodule]
fn mongojet(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // if cfg!(debug_assertions) {
    //     pyo3_log::init();
    // }

    m.add_function(wrap_pyfunction!(core_create_client, m)?)?;

    m.add_class::<CoreClient>()?;
    m.add_class::<CoreDatabase>()?;
    m.add_class::<CoreCollection>()?;
    m.add_class::<CoreCursor>()?;

    m.add("PyMongoError", m.py().get_type::<PyMongoError>())?;
    m.add("OperationFailure", m.py().get_type::<OperationFailure>())?;
    m.add("WriteError", m.py().get_type::<WriteError>())?;
    m.add("WriteConcernError", m.py().get_type::<WriteConcernError>())?;
    m.add("DuplicateKeyError", m.py().get_type::<DuplicateKeyError>())?;

    m.add("BsonSerializationError", m.py().get_type::<BsonSerializationError>())?;
    m.add("BsonDeserializationError", m.py().get_type::<BsonDeserializationError>())?;

    m.add("ConnectionFailure", m.py().get_type::<ConnectionFailure>())?;
    m.add("ServerSelectionError", m.py().get_type::<ServerSelectionError>())?;

    m.add("ConfigurationError", m.py().get_type::<ConfigurationError>())?;

    m.add("GridFSError", m.py().get_type::<GridFSError>())?;
    m.add("NoFile", m.py().get_type::<NoFile>())?;
    m.add("FileExists", m.py().get_type::<FileExists>())?;

    Ok(())
}
