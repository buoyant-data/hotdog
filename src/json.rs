//! This module acts as a shim between serde_json and simd-json to allow for higher performance JSON
//! parsing on SIMD-capable architectures

pub fn from_str<'a, S: serde::Deserialize<'a>>(buffer: &'a mut str) -> Result<S, std::io::Error> {
    #[cfg(feature = "simd")]
    unsafe {
        simd_json::serde::from_str::<S>(buffer)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    #[cfg(not(feature = "simd"))]
    {
        serde_json::from_str::<S>(buffer).map_err(|e| e.into())
    }
}

pub fn to_string<S: serde::Serialize>(value: &S) -> Result<String, serde_json::error::Error> {
    serde_json::to_string::<S>(value)
}
