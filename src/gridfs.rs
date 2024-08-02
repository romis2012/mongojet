use crate::document::CoreDocument;
use crate::error::MongoError;
use crate::options::{CoreGridFsGetByIdOptions, CoreGridFsGetByNameOptions, CoreGridFsPutOptions};
use crate::runtime::spawn;
use bson::{doc, Document};
use futures::{AsyncReadExt, AsyncWriteExt};
use log::debug;
use mongodb::gridfs::GridFsBucket;
use mongodb::options::GridFsUploadOptions;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyclass]
pub struct CoreGridFsBucket {
    bucket: GridFsBucket,
}

impl CoreGridFsBucket {
    pub fn new(bucket: GridFsBucket) -> Self {
        Self { bucket }
    }
}

#[pymethods]
impl CoreGridFsBucket {
    #[pyo3(signature = (data, options=None, metadata=None))]
    pub async fn put(
        &self,
        data: Vec<u8>,
        options: Option<CoreGridFsPutOptions>,
        metadata: Option<CoreDocument>,
    ) -> PyResult<CoreDocument> {
        let bucket = self.bucket.clone();

        debug!(
            "gridfs.put, options: {:?}, metadata: {:?}",
            options, metadata
        );

        let metadata: Option<Document> = metadata.map(Into::into);
        let upload_options = GridFsUploadOptions::builder().metadata(metadata).build();

        // let file_id = options.clone().and_then(|o| o.file_id);
        let file_id = options.as_ref().and_then(|o| o.file_id.clone());
        let filename = options.and_then(|o| o.filename).unwrap_or_default();

        let fut = async move {
            let mut upload_stream = if let Some(id) = file_id {
                bucket
                    .open_upload_stream(filename)
                    .id(id)
                    .with_options(upload_options)
                    .await
                    .map_err(|e| MongoError::from(e))?
            } else {
                bucket
                    .open_upload_stream(filename)
                    .with_options(upload_options)
                    .await
                    .map_err(|e| MongoError::from(e))?
            };

            upload_stream
                .write_all(&data[..])
                .await
                .map_err(|e| MongoError::from(e))?;

            upload_stream
                .close()
                .await
                .map_err(|e| MongoError::from(e))?;

            let result: CoreDocument = doc! {"file_id": upload_stream.id()}.into();
            Ok(result)
        };

        spawn(fut).await?
    }

    pub async fn get_by_id(&self, options: CoreGridFsGetByIdOptions) -> PyResult<PyObject> {
        let bucket = self.bucket.clone();

        debug!("gridfs.get_by_id, options: {:?}", options);

        let file_id = options.file_id;

        let fut = async move {
            let mut buf = Vec::new();
            let mut download_stream = bucket
                .open_download_stream(file_id)
                .await
                .map_err(|e| MongoError::from(e))?;
            download_stream
                .read_to_end(&mut buf)
                .await
                .map_err(|e| MongoError::from(e))?;

            // Python::with_gil(|py|{ Ok(buf.into_py(py)) })
            Python::with_gil(|py| Ok(PyBytes::new_bound(py, &buf).to_object(py)))
        };

        spawn(fut).await?
    }

    pub async fn get_by_name<'py>(
        &self,
        options: CoreGridFsGetByNameOptions,
    ) -> PyResult<PyObject> {
        let bucket = self.bucket.clone();

        debug!("gridfs.get_by_name, options: {:?}", options);

        let filename = options.filename;

        let fut = async move {
            let mut buf = Vec::new();
            let mut download_stream = bucket
                .open_download_stream_by_name(filename)
                .await
                .map_err(|e| MongoError::from(e))?;
            download_stream
                .read_to_end(&mut buf)
                .await
                .map_err(|e| MongoError::from(e))?;

            Python::with_gil(|py| Ok(PyBytes::new_bound(py, &buf).to_object(py)))
        };

        spawn(fut).await?
    }

    pub async fn delete<'py>(&self, options: CoreGridFsGetByIdOptions) -> PyResult<()> {
        let bucket = self.bucket.clone();

        debug!("gridfs.delete, options: {:?}", options);

        let file_id = options.file_id;
        let fut = async move {
            bucket
                .delete(file_id)
                .await
                .map_err(|e| MongoError::from(e))?;

            Ok(())
        };

        spawn(fut).await?
    }
}
