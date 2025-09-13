use crate::commands::{ExecutionError, ParserError};
use crate::index::IndexImpactOnCompletion::{Add, NoImpact};
use crate::index::LockType::{Read, Write};
use crate::index::{CommandCompleted, CommandIdentifier, KeyType, LockType, RedisCommandType};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Mutex;

const REDIS_STRING_COMMANDS: [&str; 6] = ["GET", "SET", "INCR", "INCRBY", "DECR", "DECRBY"];

pub (crate) struct StringExecutor {
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
        //                 INCR name
        //                 INCRBY name increment
        //                 DECR name
        //                 DECRBY name decrement

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
            "INCR" => {
                if command.len() != 2 {
                    return Err(ParserError::new("INCR command requires one parameter"));
                }
                command_type = RedisCommandType::StringCommand;
                action = "INCR".to_string();
                target = command[1].clone();
                lock_type = Write
            }
            "INCRBY" => {
                if command.len() != 3 {
                    return Err(ParserError::new("INCRBY command requires two parameter"));
                }
                command_type = RedisCommandType::StringCommand;
                action = "INCRBY".to_string();
                target = command[1].clone();
                params.push(command[2].as_bytes().to_vec().into());
                lock_type = Write
            }
            "DECR" => {
                if command.len() != 2 {
                    return Err(ParserError::new("INCR command requires one parameter"));
                }
                command_type = RedisCommandType::StringCommand;
                action = "DECR".to_string();
                target = command[1].clone();
                lock_type = Write
            }
            "DECRBY" => {
                if command.len() != 3 {
                    return Err(ParserError::new("DECRBY command requires two parameter"));
                }
                command_type = RedisCommandType::StringCommand;
                action = "DECRBY".to_string();
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

    pub fn execute_command(
        &self,
        command: &CommandIdentifier,
    ) -> Result<CommandCompleted, ExecutionError> {

        match command.get_action() {
            "GET" => {
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
            }
            "SET" => {
                let value = command.get_params()[0].clone();
                self.data.set(&command.get_target(), &value);
                Ok(CommandCompleted::new(
                    command.get_target(),
                    KeyType::String,
                    Add,
                    Bytes::from("+OK\r\n"),
                ))
            }
            "INCR" => {
               self.adjust_value_if_exists(command, 1)
            }
            "INCRBY" => {
                let value = command.get_params()[0].clone();
                let adjustment = std::str::from_utf8(&value).unwrap().parse::<i64>().unwrap();
                self.adjust_value_if_exists(command, adjustment)
            }
            "DECR" => {
                self.adjust_value_if_exists(command, -1)
            }
            "DECRBY" => {
                let value = command.get_params()[0].clone();
                let adjustment = std::str::from_utf8(&value).unwrap().parse::<i64>().unwrap();
                self.adjust_value_if_exists(command, -adjustment)
            }
            _ => {
                Err(ExecutionError::new(
                    "-WRONGTYPE Operation against a key holding the wrong kind of value",
                ))
            }
        }

    }

    fn adjust_value_if_exists(&self, command: &CommandIdentifier, adjustment: i64) -> Result<CommandCompleted, ExecutionError> {
        let updated_value: Bytes;
        let mut impact_on_index = NoImpact;
        match self.data.get(&command.get_target()) {
            Some(value) => {
                match std::str::from_utf8(&value) {
                    Ok(str_val) => {
                        match str_val.parse::<i64>() {
                            Ok(int_val) => {
                                let new_val = int_val + adjustment;
                                updated_value = Bytes::from(new_val.to_string());
                                self.data.set(&command.get_target(), &updated_value);
                            }
                            Err(_) => {
                                return Err(ExecutionError::new(
                                    "-ERR value is not an integer or out of range",
                                ));
                            }
                        }
                    }
                    Err(_) => {
                        return Err(ExecutionError::new(
                            "-ERR value is not an integer or out of range",
                        ));
                    }
                }
            }
            None => {
                updated_value = Bytes::from(adjustment.to_string());
                impact_on_index = Add;
                self.data.set(&command.get_target(), &updated_value);
            }
        }

        let mut buf = BytesMut::with_capacity(1 + updated_value.len() + 2);
        buf.extend_from_slice(b"+");
        buf.extend_from_slice(&updated_value);
        buf.extend_from_slice(b"\r\n");
        Ok(CommandCompleted::new(
            command.get_target(),
            KeyType::String,
            impact_on_index,
            buf.freeze(),
        ))
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
        setup_db_with_string(&obj);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "GET".to_string(),
            Vec::new(),
            KeyType::String,
            Read,
        );
        let result = obj.execute_command(&command);
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
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+(nil)\r\n".as_bytes());
    }

    #[test]
    fn given_key_does_not_exist_when_incr_create_key_with_value_1() {
        let db = StringExecutor::new();
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "INCR".to_string(),
            Vec::new(),
            KeyType::String,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+1\r\n");
    }

    #[test]
    fn given_valid_int_in_str_when_incr_increase_value() {
        let db = StringExecutor::new();
        setup_db_with_int(&db);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "INCR".to_string(),
            Vec::new(),
            KeyType::String,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+11\r\n");
    }

    #[test]
    fn given_valid_int_in_str_when_incrby_increase_value() {
        let db = StringExecutor::new();
        setup_db_with_int(&db);

        let mut value = Vec::new();
        value.push(Bytes::from("10"));
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "INCRBY".to_string(),
            value,
            KeyType::String,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+20\r\n");
    }

    #[test]
    fn given_valid_int_in_str_when_decr_decrease_value() {
        let db = StringExecutor::new();
        setup_db_with_int(&db);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "DECR".to_string(),
            Vec::new(),
            KeyType::String,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+9\r\n");
    }

    #[test]
    fn given_key_does_not_exist_when_decr_create_key_with_value_minus_1() {
        let db = StringExecutor::new();
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "DECR".to_string(),
            Vec::new(),
            KeyType::String,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+-1\r\n");
    }

    #[test]
    fn given_valid_int_in_str_when_decrby_decrease_value() {
        let db = StringExecutor::new();
        setup_db_with_int(&db);

        let mut value = Vec::new();
        value.push(Bytes::from("4"));
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "DECRBY".to_string(),
            value,
            KeyType::String,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+6\r\n");
    }

    #[test]
    fn given_no_key_exists_when_decrby_decrease_value() {
        let db = StringExecutor::new();
        let mut value = Vec::new();
        value.push(Bytes::from("4"));
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "DECRBY".to_string(),
            value,
            KeyType::String,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+-4\r\n");
    }

    #[test]
    fn give_string_key_when_incr_return_error() {
        let db = StringExecutor::new();
        setup_db_with_string(&db);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "INCR".to_string(),
            Vec::new(),
            KeyType::String,
            Write,
        );
        let incr_result = db.execute_command(&command);
        assert!(incr_result.is_err());
        let err = incr_result.err().unwrap();
        assert_eq!(err.get_message(), "-ERR value is not an integer or out of range");
    }


    #[test]
    fn given_non_numeric_value_when_incr_return_error() {
        let db = StringExecutor::new();
        setup_db_with_string(&db);

        // Now try to INCR the non-numeric value
        let incr_command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "INCR".to_string(),
            Vec::new(),
            KeyType::String,
            Write,
        );
        let incr_result = db.execute_command(&incr_command);
        assert!(incr_result.is_err());
        let err = incr_result.err().unwrap();
        assert_eq!(err.get_message(), "-ERR value is not an integer or out of range");
    }



    fn setup_db_with_string(db: &StringExecutor) {
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
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+OK\r\n".as_bytes());
    }

    fn setup_db_with_int(db: &StringExecutor) {
        let mut value = Vec::new();
        value.push(Bytes::from("10"));
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "SET".to_string(),
            value,
            KeyType::String,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+OK\r\n".as_bytes());
    }

}
