pub type TurboResult<T> = Result<T, TurboError>;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum TurboError {
    IO(String),
    Misc(String),
    InvalidConfig(String),
}

impl From<InternalError> for TurboError {
    fn from(value: InternalError) -> Self {
        match value {
            InternalError::IO(err) => TurboError::IO(err),
            InternalError::Misc(err) => TurboError::Misc(err),
            InternalError::InvalidFile(err) => TurboError::IO(err),
        }
    }
}

//
// Internal Error
//

pub(crate) type InternalResult<T> = Result<T, InternalError>;

#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) enum InternalError {
    IO(String),
    Misc(String),
    InvalidFile(String),
}

impl From<std::io::Error> for InternalError {
    fn from(e: std::io::Error) -> Self {
        InternalError::IO(e.to_string())
    }
}

impl std::fmt::Display for InternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InternalError::IO(msg) => write!(f, "I/O error: {}", msg),
            InternalError::Misc(msg) => write!(f, "Misc error: {}", msg),
            InternalError::InvalidFile(msg) => write!(f, "Invalid file: {}", msg),
        }
    }
}
