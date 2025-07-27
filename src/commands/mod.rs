
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
pub enum RedisCommandType {
    StringCommand,
    // Add other command types as needed
}

#[derive(Debug)]
pub struct RedisCommand {
    command_type: RedisCommandType,
    target: String,
    action: String, // which action to perform on the target
    params: Vec<String>,
}



impl RedisCommand {
    pub fn new(command_type: RedisCommandType, target: String, action: String, params: Vec<String>) -> Self {
        RedisCommand {
            command_type,
            target,
            action,
            params,
        }
    }

    pub fn to_string(&self) -> String {
        let params_str = self.params.join(" ");
        format!("{:?} {} {} {}", self.command_type, self.target, self.action, params_str)
    }


    pub fn get_command_type(&self) -> &RedisCommandType {
        &self.command_type
    }
    pub fn get_target(&self) -> &str {
        &self.target
    }

    pub fn get_action(&self) -> &str {
        &self.action
    }

    pub fn get_params(&self) -> &[String] {
        &self.params
    }
}

pub mod command {

    use super::{RedisCommand, RedisCommandType};
    use super::ParserError;
    // use crate::protocol::protocol::{Identifier, IdentifierType};

    // const REDIS_STRING_COMMANDS: [&str; 2] = ["GET", "SET"];

    //
    //
    //
    // pub fn identify_command(identifiers: &[IdentifierType]) -> Result<RedisCommand, ParserError> {
    //     use super::RedisCommand;
    //
    //     if identifiers.is_empty() {
    //         return Err(ParserError::new("No identifiers provided"));
    //     }
    //
    //     let command = &identifiers[0];
    //     match command.get_identifier() {
    //         Identifier::String(identifier) => {
    //             if is_string_command(identifier) {
    //                 return build_string_command(identifier, identifiers);
    //             } else {
    //                 Err(ParserError::new("Unsupported string command type"))
    //             }
    //         }
    //         _ => {
    //             // Handle other identifier types
    //             Err(ParserError::new("Unsupported identifier type"))
    //         }
    //     }
    // }
    //
    // fn is_string_command(command: &str) -> bool {
    //     REDIS_STRING_COMMANDS.iter().any(|&cmd| cmd.eq_ignore_ascii_case(command))
    // }
    //
    // fn build_string_command(command: &str, identifiers: &[IdentifierType]) -> Result<RedisCommand, ParserError> {
    //     // support syntax: GET name
    //     //                 SET name value
    //
    //     if identifiers.len() < 2 {
    //         return Err(ParserError::new("Not enough identifiers provided for string command"));
    //     }
    //
    //     let command_type: RedisCommandType;
    //     let target:String;
    //     let action:String;
    //     let params: mut Vec<String> = Vec::new();
    //
    //     // CMC - start here. issue is that the vector must be of the same type. Does the protocol care about the integer type, or are all of these bulk strings?
    //
    //     match command {
    //         "GET" => {
    //             if identifiers.len() != 2 {
    //                 return Err(ParserError::new("GET command requires exactly one parameter"));
    //             }
    //             command_type = RedisCommandType::StringCommand;
    //             action = "GET".to_string();
    //             match (identifiers[1].get_identifier()) {
    //                 Identifier::String(name) => {
    //                     target = name.clone();
    //                     params = vec![];
    //                 },
    //                 _ => return Err(ParserError::new("GET command requires a string parameter")),
    //             }
    //         },
    //         "SET" => {
    //             if identifiers.len() != 3 {
    //                 return Err(ParserError::new("SET command requires two parameter"));
    //             }
    //             command_type = RedisCommandType::StringCommand;
    //             action = "SET".to_string();
    //             match (identifiers[1].get_identifier()) {
    //                 Identifier::String(name) => {
    //                     target = name.clone();
    //                     params = vec![];
    //                 },
    //                 _ => return Err(ParserError::new("GET command requires a string parameter")),
    //             }
    //         },
    //         _ => return Err(ParserError::new("Unsupported string command type")),
    //     }
    //
    //
    //     Ok(RedisCommand::new(command_type, target, action, params))
    // }
}