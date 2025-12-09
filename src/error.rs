pub(crate) type InternalResult<T> = Result<T, InternalError>;

pub(crate) enum InternalError {
    IO(String),
    InvalidPath(String),
    Misc(String),
}

impl From<std::io::Error> for InternalError {
    fn from(e: std::io::Error) -> Self {
        InternalError::IO(e.to_string())
    }
}

impl std::fmt::Display for InternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IO(msg) => write!(f, "{msg}"),
            Self::InvalidPath(msg) => write!(f, "{msg}"),
            Self::Misc(msg) => write!(f, "{msg}"),
        }
    }
}
