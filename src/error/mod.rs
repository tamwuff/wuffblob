// Copied out of the book because I want to follow all applicable standards...

#[derive(Debug, Clone)]
pub struct WuffError {
    pub message: String,
}

impl std::fmt::Display for WuffError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for WuffError {}

impl From<&str> for WuffError {
    fn from(message: &str) -> WuffError {
        WuffError {
            message: String::from(message),
        }
    }
}

impl From<&String> for WuffError {
    fn from(message: &String) -> WuffError {
        WuffError {
            message: message.clone(),
        }
    }
}

impl From<String> for WuffError {
    fn from(message: String) -> WuffError {
        WuffError { message: message }
    }
}

impl From<&std::io::Error> for WuffError {
    fn from(err: &std::io::Error) -> WuffError {
        WuffError {
            message: format!("{:?}", err),
        }
    }
}

impl From<std::io::Error> for WuffError {
    fn from(err: std::io::Error) -> WuffError {
        WuffError {
            message: format!("{:?}", err),
        }
    }
}

impl From<&regex::Error> for WuffError {
    fn from(err: &regex::Error) -> WuffError {
        WuffError {
            message: format!("{:?}", err),
        }
    }
}

impl From<regex::Error> for WuffError {
    fn from(err: regex::Error) -> WuffError {
        WuffError {
            message: format!("{:?}", err),
        }
    }
}

impl From<&azure_core::error::Error> for WuffError {
    fn from(err: &azure_core::error::Error) -> WuffError {
        WuffError {
            message: format!("{:?}", err),
        }
    }
}

impl From<azure_core::error::Error> for WuffError {
    fn from(err: azure_core::error::Error) -> WuffError {
        WuffError {
            message: format!("{:?}", err),
        }
    }
}

impl From<&tokio::task::JoinError> for WuffError {
    fn from(err: &tokio::task::JoinError) -> WuffError {
        WuffError {
            message: format!("{:?}", err),
        }
    }
}

impl From<tokio::task::JoinError> for WuffError {
    fn from(err: tokio::task::JoinError) -> WuffError {
        WuffError {
            message: format!("{:?}", err),
        }
    }
}

impl<T> From<&tokio::sync::mpsc::error::SendError<T>> for WuffError {
    fn from(err: &tokio::sync::mpsc::error::SendError<T>) -> WuffError {
        WuffError {
            message: format!("{:?}", err),
        }
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for WuffError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> WuffError {
        WuffError {
            message: format!("{:?}", err),
        }
    }
}
