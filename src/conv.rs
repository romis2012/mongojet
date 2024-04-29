#[rustfmt::skip]
macro_rules! from_py_object {
    ($t:ident) => {
        impl<'py> FromPyObject<'py> for $t {
            fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
                let bytes = ob.extract::<&[u8]>()?;
                let result = bson::from_slice(bytes)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                Ok(result)
            }
        }
    };
}

#[rustfmt::skip]
macro_rules! into_py_object {
    ($t:ident) => {
        impl IntoPy<PyObject> for $t {
            fn into_py(self, py: Python<'_>) -> PyObject {
                // bson::to_vec(&self)
                //     .expect(format!("Couldn't serialize value to bson: {:?}", self).as_str())
                //     .into_py(py) // list[int]
                let buf = bson::to_vec(&self)
                    .expect(format!("Couldn't serialize value to bson: {:?}", self).as_str());
                PyBytes::new_bound(py, &buf).into()
            }
        }
    };
}

pub(crate) use from_py_object;
pub(crate) use into_py_object;
