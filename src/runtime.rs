use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::future::Future;

pub fn tokio() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

pub async fn spawn<F>(future: F) -> PyResult<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio()
        .spawn(future)
        .await
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}
