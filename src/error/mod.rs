// Copied out of the book because I want to follow all applicable standards...

#[derive(Debug, Clone)]
pub struct WuffBlobError {
    pub message: String,
}

pub type WuffBlobResult = Result<(), WuffBlobError>;

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
