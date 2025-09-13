// TODO add   LSET, LREM, LRANGE
// TODO add support for multiple adds for LPUSH and RPUSH, RPOP and LPOP

use crate::commands::{ExecutionError, ParserError};
use crate::index::IndexImpactOnCompletion::{Add, Delete, NoImpact};
use crate::index::LockType::{Read, Write};
use crate::index::{CommandCompleted, CommandIdentifier, KeyType, LockType, RedisCommandType};
use bytes::{Bytes, BytesMut};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

const REDIS_LIST_COMMANDS: [&str; 6] = ["LLEN", "LINDEX", "RPUSH", "RPOP", "LPUSH", "LPOP"];

pub(crate) struct ListExecutor {
    data: Mutex<HashMap<String, VecDeque<Bytes>>>,
}

impl ListExecutor {
    pub(crate) fn new() -> ListExecutor {
        ListExecutor {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_command_supported(command: &str) -> bool {
        REDIS_LIST_COMMANDS
            .iter()
            .any(|&cmd| cmd.eq_ignore_ascii_case(command))
    }

    pub fn build_command(command: &Vec<String>) -> Result<CommandIdentifier, ParserError> {
        // support syntax: LLEN name

        if command.len() < 2 {
            return Err(ParserError::new(
                "Not enough identifiers provided for List command",
            ));
        }

        let command_type: RedisCommandType;
        let target: String;
        let action: String;
        let lock_type: LockType;
        let mut params: Vec<Bytes> = Vec::new();

        match command[0].to_uppercase().as_str() {
            "LLEN" => {
                if command.len() != 2 {
                    return Err(ParserError::new(
                        "LLEN command requires exactly one parameter",
                    ));
                }
                command_type = RedisCommandType::ListCommand;
                action = "LLEN".to_string();
                target = command[1].clone();
                //  no params for LLEN command
                lock_type = Read
            }
            "LINDEX" => {
                if command.len() != 3 {
                    return Err(ParserError::new(
                        "LINDEX command requires exactly two parameters",
                    ));
                }
                command_type = RedisCommandType::ListCommand;
                action = "LINDEX".to_string();
                target = command[1].clone();
                params.push(command[2].as_bytes().to_vec().into());
                lock_type = Read
            }
            "RPUSH" => {
                if command.len() != 3 {
                    return Err(ParserError::new(
                        "RPUSH command requires exactly two parameters",
                    ));
                }
                command_type = RedisCommandType::ListCommand;
                action = "RPUSH".to_string();
                target = command[1].clone();
                params.push(command[2].as_bytes().to_vec().into());
                lock_type = Write
            }
            "RPOP" => {
                if command.len() != 2 {
                    return Err(ParserError::new(
                        "RPOP command requires exactly one parameters",
                    ));
                }
                command_type = RedisCommandType::ListCommand;
                action = "RPOP".to_string();
                target = command[1].clone();
                lock_type = Write
            }
            "LPUSH" => {
                if command.len() != 3 {
                    return Err(ParserError::new(
                        "LPUSH command requires exactly two parameters",
                    ));
                }
                command_type = RedisCommandType::ListCommand;
                action = "LPUSH".to_string();
                target = command[1].clone();
                params.push(command[2].as_bytes().to_vec().into());
                lock_type = Write
            }
            "LPOP" => {
                if command.len() != 2 {
                    return Err(ParserError::new(
                        "LPOP command requires exactly one parameters",
                    ));
                }
                command_type = RedisCommandType::ListCommand;
                action = "LPOP".to_string();
                target = command[1].clone();
                lock_type = Write
            }

            _ => return Err(ParserError::new("Unsupported List command type")),
        }

        Ok(CommandIdentifier::new(
            command_type,
            target,
            action,
            params,
            KeyType::List,
            lock_type,
        ))
    }

    pub fn execute_command(
        &self,
        command: &CommandIdentifier,
    ) -> Result<CommandCompleted, ExecutionError> {
        match command.get_action() {
            "LLEN" => {
                let index = self.data.lock().unwrap();
                let values = index.get(command.get_target());
                let length = match values {
                    Some(entry) => entry.len(),
                    None => 0,
                };

                Ok(CommandCompleted::new(
                    command.get_target(),
                    KeyType::List,
                    NoImpact,
                    Self::format_size_response(length),
                ))
            }
            "LINDEX" => {
                let values = self.data.lock().unwrap();
                let entries = values.get(command.get_target());
                let response: Bytes;
                match entries {
                    Some(entry) => {
                        let index = Self::index_from_bytes(&command.get_params()[0])?;
                        response = entry
                            .get(index as usize)
                            .map_or(Self::format_null_response(), |value| {
                                Self::format_string_response(value)
                            })
                    }
                    None => {
                        response = Self::format_null_response();
                    }
                }

                Ok(CommandCompleted::new(
                    command.get_target(),
                    KeyType::List,
                    NoImpact,
                    response,
                ))
            }
            "RPUSH" => {
                let mut values = self.data.lock().unwrap();
                let mut index_impact = NoImpact;
                let entries = match values.get_mut(command.get_target()) {
                    Some(entry) => entry,
                    None => {
                        let new_entry = VecDeque::new();
                        values.insert(command.get_target().parse().unwrap(), new_entry);
                        index_impact = Add;
                        values.get_mut(command.get_target()).unwrap()
                    }
                };
                entries.push_back(command.get_params()[0].clone());
                let length = entries.len();

                Ok(CommandCompleted::new(
                    command.get_target(),
                    KeyType::List,
                    index_impact,
                    Self::format_size_response(length),
                ))
            }
            "RPOP" => {
                let mut values = self.data.lock().unwrap();
                let entries = values.get_mut(command.get_target());
                let mut index_impact = NoImpact;
                let response: Bytes;
                match entries {
                    Some(entry) => {
                        match entry.pop_back() {
                            Some(value) => {
                                if entry.is_empty() {
                                    values.remove(command.get_target());
                                    index_impact = Delete;
                                }
                                response = Self::format_string_response(&value);
                            }
                            _ => {
                                response = Self::format_null_response();
                            }
                        }
                    }
                    None => {
                        response = Self::format_null_response();
                    }
                }


                Ok(CommandCompleted::new(
                    command.get_target(),
                    KeyType::List,
                    index_impact,
                    response,
                ))
            }
            "LPUSH" => {
                let mut values = self.data.lock().unwrap();
                let mut index_impact = NoImpact;
                let entries = match values.get_mut(command.get_target()) {
                    Some(entry) => entry,
                    None => {
                        let new_entry = VecDeque::new();
                        values.insert(command.get_target().parse().unwrap(), new_entry);
                        index_impact = Add;
                        values.get_mut(command.get_target()).unwrap()
                    }
                };
                entries.push_front(command.get_params()[0].clone());
                let length = entries.len();

                Ok(CommandCompleted::new(
                    command.get_target(),
                    KeyType::List,
                    index_impact,
                    Self::format_size_response(length),
                ))
            }
            "LPOP" => {
                let mut values = self.data.lock().unwrap();
                let entries = values.get_mut(command.get_target());
                let mut index_impact = NoImpact;
                let response: Bytes;
                match entries {
                    Some(entry) => {
                        match entry.pop_front() {
                            Some(value) => {
                                if entry.is_empty() {
                                    values.remove(command.get_target());
                                    index_impact = Delete;
                                }
                                response = Self::format_string_response(&value);
                            }
                            _ => {
                                response = Self::format_null_response();
                            }
                        }
                    }
                    None => {
                        response = Self::format_null_response();
                    }
                }


                Ok(CommandCompleted::new(
                    command.get_target(),
                    KeyType::List,
                    index_impact,
                    response,
                ))
            }
            _ => Err(ExecutionError::new(
                "-WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        }
    }

    fn format_size_response(size: usize) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b":");
        buf.extend_from_slice(size.to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.freeze()
    }

    fn format_string_response(value: &Bytes) -> Bytes {
        let mut buf = BytesMut::with_capacity(1 + value.len() + 2);
        buf.extend_from_slice(b"+");
        buf.extend_from_slice(&value);
        buf.extend_from_slice(b"\r\n");
        buf.freeze()
    }

    fn format_null_response() -> Bytes {
        Bytes::from("_\r\n")
    }

    fn index_from_bytes(bytes: &Bytes) -> Result<usize, ExecutionError> {
        let index_str = std::str::from_utf8(&bytes[..])
            .map_err(|_| ExecutionError::new("Invalid index format"))?;
        let index = index_str
            .parse::<isize>()
            .map_err(|_| ExecutionError::new("Index is not an integer or out of range"))?;
        Ok(index as usize)
    }

    pub(crate) fn internal_get_length(&self) -> usize {
        let values = self.data.lock().unwrap();
        values.len() as usize
    }

    pub(crate) fn internal_get_list_length(&self, key: &str) -> usize {
        let values = self.data.lock().unwrap();
        match values.get(key) {
            Some(entry) => entry.len(),
            None => 0,
        }
    }

    pub (crate) fn internal_get_list_head(&self, key: &str) -> Option<Bytes> {
        let values = self.data.lock().unwrap();
        match values.get(key) {
            Some(entry) => entry.front().cloned(),
            None => None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::index::LockType::{Read, Write};
    use crate::index::{CommandIdentifier, KeyType, RedisCommandType};
    use crate::list_executor::ListExecutor;
    use bytes::Bytes;

    #[test]
    fn given_no_list_when_llen_return_zero() {
        let db = ListExecutor::new();
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "LLEN".to_string(),
            Vec::new(),
            KeyType::List,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), ":0\r\n");
    }

    #[test]
    fn given_list_with_one_element_when_llen_return_one() {
        let db = setup_list_with_multiple_elements("key", 1);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "LLEN".to_string(),
            Vec::new(),
            KeyType::List,
            Read,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), ":1\r\n");
    }

    #[test]
    fn given_missing_list_when_lindex_return_null() {
        let db = ListExecutor::new();
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "LINDEX".to_string(),
            vec![Bytes::from("0")],
            KeyType::List,
            Read,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "_\r\n");
    }
    #[test]
    fn given_list_when_lindex_0_return_value() {
        let db = setup_list_with_multiple_elements("key", 1);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "LINDEX".to_string(),
            vec![Bytes::from("0")],
            KeyType::List,
            Read,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+Element0\r\n");
    }

    #[test]
    fn given_single_list_when_lindex_1_return_null() {
        let db = setup_list_with_multiple_elements("key", 1);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "LINDEX".to_string(),
            vec![Bytes::from("1")],
            KeyType::List,
            Read,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "_\r\n");
    }

    #[test]
    fn given_multiple_element_list_when_lindex_1_return_value() {
        let db = setup_list_with_multiple_elements("key", 2);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "LINDEX".to_string(),
            vec![Bytes::from("1")],
            KeyType::List,
            Read,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+Element1\r\n");
    }

    #[test]
    fn given_valid_list_when_lindex_with_non_numeric_error() {
        let db = setup_list_with_multiple_elements("key", 2);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "LINDEX".to_string(),
            vec![Bytes::from("a")],
            KeyType::List,
            Read,
        );
        let result = db.execute_command(&command);
        match result {
            Ok(_) => panic!("Should have returned an error"),
            Err(error) => {
                assert_eq!(
                    error.get_message(),
                    "Index is not an integer or out of range"
                );
            }
        }
    }

    #[test]
    fn given_empty_list_when_rpush_then_add_to_list() {
        let db = ListExecutor::new();
        let mut value = Vec::new();
        value.push(Bytes::from("FirstPush"));
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "RPUSH".to_string(),
            value,
            KeyType::List,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), ":1\r\n");
        assert_eq!(db.internal_get_length(), 1);
    }

    #[test]
    fn given_empty_list_when_rpop_then_return_null() {
        let db = ListExecutor::new();
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "RPOP".to_string(),
            Vec::new(),
            KeyType::List,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "_\r\n");
        assert_eq!(db.internal_get_length(), 0);
    }

    #[test]
    fn given_list_with_one_element_when_rpop_then_return_element() {
        let db = setup_list_with_multiple_elements("key", 1);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "RPOP".to_string(),
            Vec::new(),
            KeyType::List,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+Element0\r\n");
        assert_eq!(db.internal_get_length(), 0);
    }

    #[test]
    fn given_list_with_multiple_elements_when_rpop_then_return_element() {
        let db = setup_list_with_multiple_elements("key", 2);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "RPOP".to_string(),
            Vec::new(),
            KeyType::List,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+Element1\r\n");
        assert_eq!(db.internal_get_length(), 1);
        assert_eq!(db.internal_get_list_length("key"), 1);
    }

    #[test]
    fn given_existing_list_when_lpush_then_add_to_list() {
        let db = setup_list_with_multiple_elements("key", 1);
        let mut value = Vec::new();
        value.push(Bytes::from("Element-Head"));
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "LPUSH".to_string(),
           value,
            KeyType::List,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), ":2\r\n");
        assert_eq!(db.internal_get_length(), 1);
        assert_eq!(db.internal_get_list_length("key"), 2);
        assert_eq!(db.internal_get_list_head("key"), Some(Bytes::from("Element-Head")));
    }

    // test lpop pops from the head of the list
    #[test]
    fn given_existing_list_when_lpop_pop_the_head() {
        let db = setup_list_with_multiple_elements("key", 2);
        let command = CommandIdentifier::new(
            RedisCommandType::StringCommand,
            "key".to_string(),
            "LPOP".to_string(),
            Vec::new(),
            KeyType::List,
            Write,
        );
        let result = db.execute_command(&command);
        assert_eq!(result.unwrap().get_response(), "+Element0\r\n");
        assert_eq!(db.internal_get_length(), 1);
        assert_eq!(db.internal_get_list_length("key"), 1);
        assert_eq!(db.internal_get_list_head("key"), Some(Bytes::from("Element1")));
    }

    fn setup_list_with_multiple_elements(key_name: &str, size: usize) -> ListExecutor {
        let db = ListExecutor::new();
        for i in 0..size {
            let mut value = Vec::new();
            value.push(Bytes::from(format!("Element{}", i)));
            let command = CommandIdentifier::new(
                RedisCommandType::StringCommand,
                key_name.to_string(),
                "RPUSH".to_string(),
                value,
                KeyType::List,
                Write,
            );
            let _ = db.execute_command(&command);
        }
        db
    }
}
