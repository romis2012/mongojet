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
        impl<'py> IntoPyObject<'py> for $t {
            type Target = PyBytes;
            type Output = Bound<'py, Self::Target>;
            type Error = std::convert::Infallible;
        
            fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
                let buf = bson::to_vec(&self)
                    .expect(format!("Couldn't serialize value to bson: {:?}", self).as_str());
                Ok(PyBytes::new(py, &buf))
            }
        }
    };
}

pub(crate) use from_py_object;
pub(crate) use into_py_object;
