pub type TurboResult<T> = Result<T, TurboError>;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TurboError {
    IO(String),
    Misc(String),
    LockPoisoned(String),
    InvalidState(String),
    PermissionDenied(String),
}

impl std::fmt::Display for TurboError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IO(msg) => write!(f, "{msg}"),
            Self::Misc(msg) => write!(f, "{msg}"),
            Self::LockPoisoned(msg) => write!(f, "{msg}"),
            Self::InvalidState(msg) => write!(f, "{msg}"),
            Self::PermissionDenied(msg) => write!(f, "{msg}"),
        }
    }
}

impl From<InternalError> for TurboError {
    fn from(err: InternalError) -> Self {
        match err {
            InternalError::IO(e) => Self::IO(e),
            InternalError::LockPoisoned(e) => Self::LockPoisoned(e),
            InternalError::InvalidState(e) => Self::InvalidState(e),
            InternalError::PermissionDenied(e) => Self::PermissionDenied(e),
        }
    }
}

pub(crate) type InternalResult<T> = Result<T, InternalError>;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum InternalError {
    IO(String),
    LockPoisoned(String),
    InvalidState(String),
    PermissionDenied(String),
}

impl From<std::io::Error> for InternalError {
    fn from(e: std::io::Error) -> Self {
        InternalError::IO(format!("{}", e))
    }
}

impl<T> From<std::sync::PoisonError<T>> for InternalError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        InternalError::LockPoisoned(e.to_string())
    }
}

impl std::fmt::Display for InternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IO(msg) => write!(f, "{msg}"),
            Self::LockPoisoned(msg) => write!(f, "{msg}"),
            Self::InvalidState(msg) => write!(f, "{msg}"),
            Self::PermissionDenied(msg) => write!(f, "{msg}"),
        }
    }
}
