use bson::{Document, RawDocumentBuf};
use mongodb::options::UpdateModifications;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[derive(Debug, Clone)]
pub struct CoreDocument(pub Document);

impl From<Document> for CoreDocument {
    fn from(value: Document) -> Self {
        Self(value)
    }
}

impl Into<Document> for CoreDocument {
    fn into(self) -> Document {
        self.0
    }
}

#[rustfmt::skip]
impl<'py> FromPyObject<'_, 'py> for CoreDocument {
    type Error = PyErr;
    
    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let mut data = obj.extract::<&[u8]>()?;
        let doc = Document::from_reader(&mut data)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        Ok(CoreDocument(doc))
    }
}

impl<'py> IntoPyObject<'py> for CoreDocument {
    type Target = PyBytes;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let mut data: Vec<u8> = Vec::new();
        self.0
            .to_writer(&mut data)
            .expect("Couldn't convert bson document into bytes");

        Ok(PyBytes::new(py, &data))
    }
}

#[derive(Debug, Clone)]
pub struct CorePipeline(Vec<Document>);

impl From<Vec<Document>> for CorePipeline {
    fn from(value: Vec<Document>) -> Self {
        CorePipeline(value)
    }
}

impl Into<Vec<Document>> for CorePipeline {
    fn into(self) -> Vec<Document> {
        self.0
    }
}

impl<'py> FromPyObject<'_, 'py> for CorePipeline {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let list = obj.extract::<Vec<Vec<u8>>>()?; //list of bytes
        let mut result = Vec::with_capacity(list.len());

        for bytes in list.into_iter() {
            let mut data = bytes.as_slice();
            let doc = Document::from_reader(&mut data)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            result.push(doc)
        }

        Ok(CorePipeline(result))
    }
}

impl IntoIterator for CorePipeline {
    type Item = Document;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, Clone)]
pub enum CoreCompoundDocument {
    Doc(CoreDocument),
    Pipeline(CorePipeline),
}

impl<'py> FromPyObject<'_, 'py> for CoreCompoundDocument {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(pipeline) = obj.extract::<CorePipeline>() {
            return Ok(CoreCompoundDocument::Pipeline(pipeline));
        }
        if let Ok(doc) = obj.extract::<CoreDocument>() {
            return Ok(CoreCompoundDocument::Doc(doc));
        }
        Err(PyValueError::new_err(
            "Couldn't convert CoreCompoundDocument from python".to_string(),
        ))
    }
}

impl Into<UpdateModifications> for CoreCompoundDocument {
    fn into(self) -> UpdateModifications {
        match self {
            CoreCompoundDocument::Doc(doc) => UpdateModifications::Document(doc.into()),
            CoreCompoundDocument::Pipeline(list) => UpdateModifications::Pipeline(list.into()),
        }
    }
}

////////////////////////////////////////
#[derive(Debug, Clone)]
pub struct CoreRawDocument(RawDocumentBuf);

impl From<RawDocumentBuf> for CoreRawDocument {
    fn from(value: RawDocumentBuf) -> Self {
        Self(value)
    }
}

impl Into<RawDocumentBuf> for CoreRawDocument {
    fn into(self) -> RawDocumentBuf {
        self.0
    }
}

impl<'py> IntoPyObject<'py> for CoreRawDocument {
    type Target = PyBytes;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyBytes::new(py, self.0.as_bytes()))
    }
}

impl<'py> FromPyObject<'_, 'py> for CoreRawDocument {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let data = obj.extract::<&[u8]>()?;
        let doc = RawDocumentBuf::from_bytes(data.into())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(CoreRawDocument(doc))
    }
}
