pub(crate) type InternalResult<T> = Result<T, InternalError>;

#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) enum InternalError {
    Io(String),
}

impl From<std::io::Error> for InternalError {
    fn from(e: std::io::Error) -> Self {
        InternalError::Io(e.to_string())
    }
}
