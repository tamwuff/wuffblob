// Copied out of the book because I want to follow all applicable standards...

#[derive(Debug, Clone)]
pub struct WuffBlobError {
    pub message: String,
}

impl std::fmt::Display for WuffBlobError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for WuffBlobError {}

impl From<&str> for WuffBlobError {
    fn from(message: &str) -> WuffBlobError {
        WuffBlobError {
            message: String::from(message),
        }
    }
}

impl From<&String> for WuffBlobError {
    fn from(message: &String) -> WuffBlobError {
        WuffBlobError {
            message: message.clone(),
        }
    }
}

impl From<String> for WuffBlobError {
    fn from(message: String) -> WuffBlobError {
        WuffBlobError { message: message }
    }
}

impl From<&azure_core::error::Error> for WuffBlobError {
    fn from(err: &azure_core::error::Error) -> WuffBlobError {
        WuffBlobError {
            message: format!("{:?}", err),
        }
    }
}

impl From<azure_core::error::Error> for WuffBlobError {
    fn from(err: azure_core::error::Error) -> WuffBlobError {
        WuffBlobError {
            message: format!("{:?}", err),
        }
    }
}

impl From<&tokio::task::JoinError> for WuffBlobError {
    fn from(err: &tokio::task::JoinError) -> WuffBlobError {
        WuffBlobError {
            message: format!("{:?}", err),
        }
    }
}

impl From<tokio::task::JoinError> for WuffBlobError {
    fn from(err: tokio::task::JoinError) -> WuffBlobError {
        WuffBlobError {
            message: format!("{:?}", err),
        }
    }
}

impl<T> From<&tokio::sync::mpsc::error::SendError<T>> for WuffBlobError {
    fn from(err: &tokio::sync::mpsc::error::SendError<T>) -> WuffBlobError {
        WuffBlobError {
            message: format!("{:?}", err),
        }
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for WuffBlobError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> WuffBlobError {
        WuffBlobError {
            message: format!("{:?}", err),
        }
    }
}
