pub type TurboResult<T> = Result<T, TurboError>;

#[derive(Debug, PartialEq, Eq)]
pub enum TurboError {
    IO(String),
    InvalidPath(String),
    InvalidConfig(String),
    InvalidDbState(String),
    PermissionDenied(String),
}

impl From<InternalError> for TurboError {
    fn from(err: InternalError) -> Self {
        match err {
            InternalError::IO(e) => Self::IO(e),
            InternalError::InvalidPath(e) => Self::InvalidPath(e),
            InternalError::InvalidConfig(e) => Self::InvalidConfig(e),
            InternalError::InvalidDbState(e) => Self::InvalidDbState(e),
            InternalError::PermissionDenied(e) => Self::PermissionDenied(e),
        }
    }
}

pub(crate) type InternalResult<T> = Result<T, InternalError>;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum InternalError {
    IO(String),
    InvalidPath(String),
    InvalidConfig(String),
    InvalidDbState(String),
    PermissionDenied(String),
}

impl From<std::io::Error> for InternalError {
    fn from(e: std::io::Error) -> Self {
        InternalError::IO(format!("{}", e))
    }
}

impl std::fmt::Display for InternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IO(msg) => write!(f, "{msg}"),
            Self::InvalidPath(msg) => write!(f, "{msg}"),
            Self::InvalidConfig(msg) => write!(f, "{msg}"),
            Self::InvalidDbState(msg) => write!(f, "{msg}"),
            Self::PermissionDenied(msg) => write!(f, "{msg}"),
        }
    }
}
