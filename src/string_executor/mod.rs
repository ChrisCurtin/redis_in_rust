pub mod string_storage;

use crate::commands::{ExecutionError, ParserError, RedisCommand, RedisCommandType};
use crate::string_executor::string_storage::StringStorage;
use bytes::{Bytes, BytesMut};

pub fn execute_string_command(
    db: &StringStorage,
    command: &RedisCommand,
) -> Result<Bytes, ExecutionError> {
    if command.get_action() == "GET" {
        match db.get(&command.get_target()) {
            Some(value) => {
                let mut buf = BytesMut::with_capacity(1 + value.len() + 2);
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(&value);
                buf.extend_from_slice(b"\r\n");
                Ok(buf.freeze())
            },
            None => Ok(Bytes::from("+(nil)\r\n"))
        }

    } else if command.get_action() == "SET" {
        let value = command.get_params()[0].clone();
        db.set(&command.get_target(), &value);
        Ok(Bytes::from("+OK\r\n"))
    } else {
        Err(ExecutionError::new(
            "-WRONGTYPE Operation against a key holding the wrong kind of value",
        ))
    }
}

const REDIS_STRING_COMMANDS: [&str; 2] = ["GET", "SET"];

pub fn is_string_command(command: &str) -> bool {
    REDIS_STRING_COMMANDS
        .iter()
        .any(|&cmd| cmd.eq_ignore_ascii_case(command))
}

pub fn build_string_command(identifiers: &Vec<String>) -> Result<RedisCommand, ParserError> {
    // support syntax: GET name
    //                 SET name value

    if identifiers.len() < 2 {
        return Err(ParserError::new(
            "Not enough identifiers provided for string command",
        ));
    }

    let command_type: RedisCommandType;
    let target: String;
    let action: String;
    let mut params: Vec<Bytes> = Vec::new();

    match identifiers[0].to_uppercase().as_str() {
        "GET" => {
            if identifiers.len() != 2 {
                return Err(ParserError::new(
                    "GET command requires exactly one parameter",
                ));
            }
            command_type = RedisCommandType::StringCommand;
            action = "GET".to_string();
            target = identifiers[1].clone();
            // not no params for GET command
        }
        "SET" => {
            if identifiers.len() != 3 {
                return Err(ParserError::new("SET command requires two parameter"));
            }
            command_type = RedisCommandType::StringCommand;
            action = "SET".to_string();
            target = identifiers[1].clone();
            params.push(identifiers[2].as_bytes().to_vec().into());
        }
        _ => return Err(ParserError::new("Unsupported string command type")),
    }

    Ok(RedisCommand::new(command_type, target, action, params))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use crate::commands::{RedisCommand, RedisCommandType};
    use crate::string_executor::execute_string_command;
    use crate::string_executor::string_storage::StringStorage;

    #[test]
    fn given_valid_key_when_get_return_value() {
        let db = StringStorage::new();
        setup_db(&db);
        let command = RedisCommand::new(RedisCommandType::StringCommand, "key".to_string(), "GET".to_string(), Vec::new());
        let result = execute_string_command(&db, &command);
        assert_eq!(result.unwrap(), "+value\r\n".as_bytes());
    }

    #[test]
    fn given_empty_db_when_get_return_empty_string() {
        let db = StringStorage::new();
        let command = RedisCommand::new(RedisCommandType::StringCommand, "key".to_string(), "GET".to_string(), Vec::new());
        let result = execute_string_command(&db, &command);
        assert_eq!(result.unwrap(), "_\r\n".as_bytes());
    }

    fn setup_db(db: & StringStorage) {
        let mut value = Vec::new();
        value.push(Bytes::from("value"));
        let command = RedisCommand::new(RedisCommandType::StringCommand, "key".to_string(), "SET".to_string(), value);
        let result = execute_string_command(&db, &command);
        assert_eq!(result.unwrap(), Bytes::from("+OK\r\n") );
    }
}
