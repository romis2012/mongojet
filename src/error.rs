use std::fmt::{Display, Formatter};

use mongodb::error::{ErrorKind, GridFsErrorKind, WriteFailure};
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::{create_exception, PyErr};

create_exception!(
    mongojet,
    PyMongoError,
    PyException,
    "Base class for all PyMongo exceptions"
);

create_exception!(
    mongojet,
    OperationFailure,
    PyMongoError,
    "Raised when a database operation fails"
);
create_exception!(
    mongojet,
    WriteError,
    OperationFailure,
    "Base exception type for errors raised during write operations"
);
create_exception!(
    mongojet,
    WriteConcernError,
    OperationFailure,
    "Base exception type for errors raised due to write concern"
);
create_exception!(
    mongojet,
    DuplicateKeyError,
    WriteError,
    "Raised when an insert or update fails due to a duplicate key error"
);

create_exception!(
    mongojet,
    BsonSerializationError,
    PyMongoError,
    "SerializationError"
);
create_exception!(
    mongojet,
    BsonDeserializationError,
    PyMongoError,
    "DeserializationError"
);

create_exception!(
    mongojet,
    ConnectionFailure,
    PyMongoError,
    "Raised when a connection to the database cannot be made or is lost."
);
create_exception!(
    mongojet,
    ServerSelectionError,
    ConnectionFailure,
    "Thrown when no MongoDB server is available for an operation"
);

create_exception!(
    mongojet,
    ConfigurationError,
    PyMongoError,
    "Raised when something is incorrectly configured"
);

create_exception!(
    mongojet,
    GridFSError,
    PyMongoError,
    "Base class for all GridFS exceptions"
);

create_exception!(
    mongojet,
    NoFile,
    GridFSError,
    "Raised when trying to read from a non-existent file"
);

create_exception!(
    mongojet,
    FileExists,
    GridFSError,
    "Raised when trying to create a file that already exists"
);

#[derive(Debug, Clone)]
pub struct MongoError(mongodb::error::Error);

impl Display for MongoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<mongodb::error::Error> for MongoError {
    fn from(value: mongodb::error::Error) -> Self {
        MongoError(value)
    }
}

impl From<MongoError> for PyErr {
    fn from(value: MongoError) -> Self {
        let msg = value.clone().to_string();
        match *value.0.kind {
            // ErrorKind::InvalidArgument { .. } => ConfigurationError::new_err(msg),
            ErrorKind::InvalidArgument { .. } => PyValueError::new_err(msg),
            ErrorKind::Authentication { .. } => ConfigurationError::new_err(msg),
            ErrorKind::BsonSerialization(..) => BsonSerializationError::new_err(msg),
            ErrorKind::BsonDeserialization(..) => BsonDeserializationError::new_err(msg),
            ErrorKind::ServerSelection { .. } => ServerSelectionError::new_err(msg),
            ErrorKind::Write(failure) => match failure {
                WriteFailure::WriteConcernError(_) => WriteConcernError::new_err(msg),
                WriteFailure::WriteError(w) => {
                    //todo: more specific error for different error codes
                    if w.code == 11000 {
                        DuplicateKeyError::new_err(msg)
                    } else {
                        WriteError::new_err(msg)
                    }
                }
                _ => WriteError::new_err(msg),
            },
            ErrorKind::BulkWrite(..) => WriteError::new_err(msg),
            ErrorKind::Command(..) => {
                //todo: more specific error for different error codes
                // if cmd.code == 85{
                OperationFailure::new_err(msg)
            }
            ErrorKind::GridFs(kind) => match kind {
                GridFsErrorKind::FileNotFound { .. } => NoFile::new_err(msg),
                _ => GridFSError::new_err(msg),
            },
            _ => PyMongoError::new_err(msg),
        }
    }
}

impl From<std::io::Error> for MongoError {
    fn from(io_error: std::io::Error) -> Self {
        let mongo_error = io_error.downcast::<mongodb::error::Error>();
        match mongo_error {
            Ok(e) => MongoError(e),
            Err(e) => MongoError(mongodb::error::Error::from(e)),
        }
    }
}
