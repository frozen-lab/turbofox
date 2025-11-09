pub(crate) type InternalResult<T> = Result<T, InternalError>;

#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) enum InternalError {
    IO(String),
    Misc(String),
}

impl From<std::io::Error> for InternalError {
    fn from(e: std::io::Error) -> Self {
        InternalError::IO(e.to_string())
    }
}
