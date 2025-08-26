use std::convert::From;
#[derive(Debug)]
pub struct ParserError {
    message: String,
}

impl ParserError {
    pub fn new(message: &str) -> Self {
        ParserError {
            message: message.to_string(),
        }
    }
    pub fn get_message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug)]
pub struct ExecutionError {
    message: String,
}

impl ExecutionError {
    pub fn new(message: &str) -> Self {
        ExecutionError {
            message: message.to_string(),
        }
    }
    pub fn get_message(&self) -> &str {
        &self.message
    }
}

impl From<ParserError> for ExecutionError {
    fn from(e: ParserError) -> Self {
        ExecutionError {
            message: e.message,
        }
    }
}

