use crate::commands::{ExecutionError, ParserError};
use crate::index::IndexImpactOnCompletion::{Add, NoImpact};
use crate::index::LockType::{Read, Write};
use crate::index::{CommandCompleted, CommandIdentifier, KeyType, LockType, RedisCommandType};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Mutex;

const REDIS_STRING_COMMANDS: [&str; 2] = ["GET", "SET"];

pub struct StringExecutor {
    data: InternalStorage,
}

impl StringExecutor {
    pub(crate) fn new() -> StringExecutor {
        StringExecutor {
            data: InternalStorage::new(),
        }
    }

    pub fn is_command_supported(command: &str) -> bool {
        REDIS_STRING_COMMANDS
            .iter()
            .any(|&cmd| cmd.eq_ignore_ascii_case(command))
    }

    pub fn build_command(command: &Vec<String>) -> Result<CommandIdentifier, ParserError> {
        // support syntax: GET name
        //                 SET name value

        if command.len() < 2 {
            return Err(ParserError::new(
                "Not enough identifiers provided for string command",
            ));
        }

        let command_type: RedisCommandType;
        let target: String;
        let action: String;
        let lock_type: LockType;
        let mut params: Vec<Bytes> = Vec::new();

        match command[0].to_uppercase().as_str() {
            "GET" => {
                if command.len() != 2 {
                    return Err(ParserError::new(
                        "GET command requires exactly one parameter",
                    ));
                }
                command_type = RedisCommandType::StringCommand;
                action = "GET".to_string();
                target = command[1].clone();
                // not no params for GET command
                lock_type = Read
            }
            "SET" => {
                if command.len() != 3 {
                    return Err(ParserError::new("SET command requires two parameter"));
                }
                command_type = RedisCommandType::StringCommand;
                action = "SET".to_string();
                target = command[1].clone();
                params.push(command[2].as_bytes().to_vec().into());
                lock_type = Write
            }
            _ => return Err(ParserError::new("Unsupported string command type")),
        }

        Ok(CommandIdentifier::new(
            command_type,
            target,
            action,
            params,
            KeyType::String,
            lock_type,
        ))
    }

    pub fn execute_string_command(
        &self,
        command: &CommandIdentifier,
    ) -> Result<CommandCompleted, ExecutionError> {
        if command.get_action() == "GET" {
            match self.data.get(&command.get_target()) {
                Some(value) => {
                    let mut buf = BytesMut::with_capacity(1 + value.len() + 2);
                    buf.extend_from_slice(b"+");
                    buf.extend_from_slice(&value);
                    buf.extend_from_slice(b"\r\n");
                    Ok(CommandCompleted::new(
                        command.get_target(),
                        KeyType::String,
                        NoImpact,
                        buf.freeze(),
                    ))
                }
                None => Ok(CommandCompleted::new(
                    command.get_target(),
                    KeyType::String,
                    NoImpact,
                    Bytes::from("+(nil)\r\n"),
                )),
            }
        } else if command.get_action() == "SET" {
            let value = command.get_params()[0].clone();
            self.data.set(&command.get_target(), &value);
            Ok(CommandCompleted::new(
                command.get_target(),
                KeyType::String,
                Add,
                Bytes::from("+OK\r\n"),
            ))
        } else {
            Err(ExecutionError::new(
                "-WRONGTYPE Operation against a key holding the wrong kind of value",
            ))
        }
    }
    
    pub fn delete(&self, key: &str) -> u16{
        self.data.del(key);
        1 // removed the single key
    }

    pub fn rename(&self, old_key: &str, new_key: &str) -> bool {
        if let Some(value) = self.data.get(old_key) {
            self.data.set(new_key, &value);
            self.data.del(old_key);
            true
        } else {
            false
        }
    }

    pub fn internal_exists(&self, key: &str) -> bool {
        // This is kind of ugly, but we need a way to confirm that the Index actually removed this key vs. only from its internal storage
        self.data.get(key).is_some()
    }

}

#[derive(Debug)]
struct Entry {
    data: Bytes,
}
#[derive(Debug)]
struct InternalStorage {
    entries: Mutex<HashMap<String, Entry>>,
}

impl InternalStorage {
    fn new() -> InternalStorage {
        InternalStorage {
            entries: Mutex::new(HashMap::new()),
        }
    }
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let values = self.entries.lock().unwrap();
        values.get(key).map(|entry| entry.data.clone())
    }
    pub fn set(&self, key: &str, value: &Bytes) {
        let mut entries = self.entries.lock().unwrap();
        entries.insert(
            key.to_string(),
            Entry {
                data: value.clone(),
            },
        );
    }
    pub fn del(&self, key: &str) {
        let mut entries = self.entries.lock().unwrap();
        entries.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use crate::index::LockType::{Read, Write};
    use crate::index::{CommandIdentifier, KeyType, RedisCommandType};
    use crate::string_executor::StringExecutor;
    use bytes::Bytes;

    #[test]
    fn given_valid_key_when_get_return_value() {
        let obj = StringExecutor::new();
        setup_db(&obj);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "GET".to_string(),
            Vec::new(),
            KeyType::String,
            Read,
        );
        let result = obj.execute_string_command(&command);
        assert_eq!(result.unwrap().get_response(), "+value\r\n".as_bytes());
    }

    #[test]
    fn given_empty_db_when_get_return_empty_string() {
        let db = StringExecutor::new();
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "GET".to_string(),
            Vec::new(),
            KeyType::String,
            Read,
        );
        let result = db.execute_string_command(&command);
        assert_eq!(result.unwrap().get_response(), "+(nil)\r\n".as_bytes());
    }

    fn setup_db(db: &StringExecutor) {
        let mut value = Vec::new();
        value.push(Bytes::from("value"));
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "SET".to_string(),
            value,
            KeyType::String,
            Write,
        );
        let result = db.execute_string_command(&command);
        assert_eq!(result.unwrap().get_response(), "+OK\r\n".as_bytes());
    }
}
