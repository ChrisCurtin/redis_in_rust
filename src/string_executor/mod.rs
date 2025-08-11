use crate::commands::{ExecutionError, ParserError, RedisCommand, RedisCommandType};

pub fn execute_string_command(command: &str) -> Result<String, ExecutionError> {
    // This function will execute the string command and return the result
    // For now, we will just return the command as a string
    // In a real implementation, this would interact with a Redis server or similar
    Ok(format!("Executed command: {}", command))
}

const REDIS_STRING_COMMANDS: [&str; 2] = ["GET", "SET"];

pub fn is_string_command(command: &str) -> bool {
    REDIS_STRING_COMMANDS.iter().any(|&cmd| cmd.eq_ignore_ascii_case(command))
}

pub fn build_string_command(command: &str, identifiers: &Vec<String>) -> Result<RedisCommand, ParserError> {
    // support syntax: GET name
    //                 SET name value

    if identifiers.len() < 2 {
        return Err(ParserError::new("Not enough identifiers provided for string command"));
    }

    let command_type: RedisCommandType;
    let target:String;
    let action:String;
    let mut params:  Vec<String> = Vec::new();

    match command {
        "GET" => {
            if identifiers.len() != 2 {
                return Err(ParserError::new("GET command requires exactly one parameter"));
            }
            command_type = RedisCommandType::StringCommand;
            action = "GET".to_string();
            target = identifiers[1].clone();
            // not no params for GET command
        },
        "SET" => {
            if identifiers.len() != 3 {
                return Err(ParserError::new("SET command requires two parameter"));
            }
            command_type = RedisCommandType::StringCommand;
            action = "SET".to_string();
            target = identifiers[1].clone();
            params.push(identifiers[2].clone());
        },
        _ => return Err(ParserError::new("Unsupported string command type")),
    }

    Ok(RedisCommand::new(command_type, target, action, params))
}