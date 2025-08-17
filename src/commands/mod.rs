use std::fmt;
use bytes::Bytes;

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

#[derive(Debug)]
pub enum RedisCommandType {
    StringCommand,
    // Add other command types as needed
}

#[derive(Debug)]
pub struct RedisCommand {
    command_type: RedisCommandType,
    target: String,
    action: String, // which action to perform on the target
    params: Vec<Bytes>,
}



impl RedisCommand {
    pub fn new(command_type: RedisCommandType, target: String, action: String, params: Vec<Bytes>) -> Self {
        RedisCommand {
            command_type,
            target,
            action,
            params,
        }
    }

    // pub fn to_string(&self) -> String {
    //     let params_str = self.params.join(" ");
    //     format!("{:?} {} {} {}", self.command_type, self.target, self.action, params_str)
    // }


    pub fn get_command_type(&self) -> &RedisCommandType {
        &self.command_type
    }
    pub fn get_target(&self) -> &str {
        &self.target
    }

    pub fn get_action(&self) -> &str {
        &self.action
    }

    pub fn get_params(&self) -> &[Bytes] {
        &self.params
    }
}

impl fmt::Display for RedisCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RedisCommand {
                command_type,
                target,
                action,
                params,
            } => {
                // TODO - figure out how to get the vector of Bytes into a string representation
                let params_str = "TODO - this is mising";
                write!(
                    f,
                    "{:?} {} {} {}",
                    command_type, target, action, params_str
                )
            }
        }
    }
}

const UNKNOWN_COMMAND: &'static str = "Unknown command";
