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
impl<'a> FromPyObject<'a> for CoreDocument {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let mut data = ob.extract::<&[u8]>()?;
        let doc = Document::from_reader(&mut data)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        Ok(CoreDocument(doc))
    }
}

impl IntoPy<PyObject> for CoreDocument {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let mut data: Vec<u8> = Vec::new();
        self.0
            .to_writer(&mut data)
            .expect("Couldn't convert bson document into bytes");
        PyBytes::new_bound(py, &data).to_object(py)
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

impl<'a> FromPyObject<'a> for CorePipeline {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let list = ob.extract::<Vec<Vec<u8>>>()?; //list of bytes
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

impl<'a> FromPyObject<'a> for CoreCompoundDocument {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if let Ok(pipeline) = ob.extract::<CorePipeline>() {
            return Ok(CoreCompoundDocument::Pipeline(pipeline));
        }
        if let Ok(doc) = ob.extract::<CoreDocument>() {
            return Ok(CoreCompoundDocument::Doc(doc));
        }
        return Err(PyValueError::new_err(
            "Couldn't convert CoreCompoundDocument from python".to_string(),
        ));
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

impl IntoPy<PyObject> for CoreRawDocument {
    fn into_py(self, py: Python<'_>) -> PyObject {
        // self.0.into_bytes().into_py(py) //list[int]
        PyBytes::new_bound(py, self.0.as_bytes()).to_object(py)
    }
}

impl<'a> FromPyObject<'a> for CoreRawDocument {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let data = ob.extract::<&[u8]>()?;
        let doc = RawDocumentBuf::from_bytes(data.into())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(CoreRawDocument(doc))
    }
}
