// List of all the keys, and their types

use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use bytes::{Bytes, BytesMut};
use crate::commands::{ExecutionError, ParserError};
use crate::controller::Databases;
use crate::index::IndexImpactOnCompletion::{Delete, NoImpact};
use crate::index::KeyType::Undefined;
use crate::index::LockType::{Read, Write};
use crate::index::RedisCommandType::{UnknownCommand, StringCommand, IndexCommand};
use crate::string_executor::StringExecutor;

// What kind of lock do we need on the Index for this command?
#[derive(Debug, PartialEq)]
pub(crate) enum LockType {
    Read,
    Write
}

// If the command successfully completes, what impact does it have on the Index?
#[derive(Debug, PartialEq, Default)]
pub enum IndexImpactOnCompletion {
    #[default]
    NoImpact,
    Add,
    Delete,
    Rename
}

#[derive(Debug, PartialEq)]
pub enum RedisCommandType {
    UnknownCommand,
    StringCommand,
    IndexCommand
    // Add other command types as needed
}

pub struct CommandIdentifier {
    command_type: RedisCommandType,
    target: String,
    action: String, // which action to perform on the target
    params: Vec<Bytes>,
    key_type: KeyType,
    lock_type: LockType
}

impl CommandIdentifier {
    
    pub fn new(command_type: RedisCommandType, target: String, action: String, params: Vec<Bytes>, key_type: KeyType, lock_type: LockType) -> CommandIdentifier {
        CommandIdentifier {
            command_type,
            target,
            action,
            params,
            key_type,
            lock_type
        }
    }
    pub fn get_command_type(&self) -> &RedisCommandType {
        &self.command_type
    }
    pub fn get_lock_type(&self) -> &LockType {
        &self.lock_type
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
    pub fn get_key_type(&self) -> &KeyType {
        &self.key_type
    }
}

#[derive(Default, Debug)]
pub(crate) struct CommandCompleted {
    key_name: String,
    key_type: KeyType,
    impact_on_index: IndexImpactOnCompletion,
    response: Bytes
}

impl CommandCompleted {
    pub fn new(key_name: &str, key_type: KeyType, impact_on_index: IndexImpactOnCompletion, response: Bytes) -> CommandCompleted {
        CommandCompleted {
            key_name: key_name.parse().unwrap(),
            key_type,
            impact_on_index,
            response
        }
    }

    pub fn get_key_name(&self) -> &String {
        &self.key_name
    }
    pub fn get_key_type(&self) -> &KeyType {
        &self.key_type
    }
    pub fn get_impact_on_index(&self) -> &IndexImpactOnCompletion {
        &self.impact_on_index
    }
    pub fn get_response(&self) -> &Bytes {
        &self.response
    }
}


const REDIS_INDEX_COMMANDS: [&str; 3] = ["EXISTS", "DEL", "RENAME"];




#[derive(Debug)]
pub struct Index {
    shared: InternalStorage
}

impl Index {
    pub fn new() -> Index {
        Index {
            shared: InternalStorage::new()
        }
    }


    pub fn execute_command(&self, databases: &Arc<Databases>, request: &Vec<String>) -> Result<Bytes, ExecutionError> {
        let command = &request[0];
        let execution_context =
            if StringExecutor::is_command_supported(&command) {
                StringExecutor::build_command(&request)?
            } else if self.is_index_command(&command) {
                self.build_index_command(&request)?
            } else {
                Err(ExecutionError::new("Unknown Command"))?
            };

        // lock the index
        {
            let mut index = self.shared.entries.lock().unwrap();
            let cmd = self.internal_execute_command(&databases, &execution_context, &mut index)?;
            Ok(cmd.get_response().clone())
        } // we unlock when we leave the block
    }

    fn internal_execute_command(&self, databases: &&Arc<Databases>, execution_context: &CommandIdentifier, index: &mut MutexGuard<HashMap<String, KeyType>>) -> Result<CommandCompleted, ExecutionError> {
        // We need to be able to modify the index in the RENAME command by possibly deleting an old key, possibly of a different type.
        // So we need to be able to manipulate the index while holding the lock for a second command.
        // This method is then called recursively in that case

        // See if the key exists in the index, then check that the types match
        //
        let key = execution_context.get_target();
        let key_type: KeyType;
        if index.contains_key(key) {
            key_type = index.get_mut(key).unwrap().clone();
            if execution_context.get_key_type() != &KeyType::Index && key_type != *execution_context.get_key_type() {
                // Index commands apply to all key types
                return Err(ExecutionError::new("Key already exists with different type"))
            }
        } else {
            key_type = Undefined;
        }

        let command_result: Result<CommandCompleted, ExecutionError> =
            match execution_context.get_command_type() {
                UnknownCommand => { Ok(CommandCompleted::default()) } // We should never get here, but we need the case to be certain all the RedisCommandTypes are covered
                StringCommand => {
                    StringExecutor::execute_string_command(&databases.string, &execution_context)
                }
                IndexCommand => {
                    self.execute_index_command(index, &databases, &execution_context, &key_type)
                }
            };

        let cmd = command_result?;
        match cmd.get_impact_on_index() {
            NoImpact => {}
            IndexImpactOnCompletion::Add => {
                index.insert(cmd.get_key_name().clone(), cmd.get_key_type().clone());
            }
            Delete => {
                index.remove(cmd.get_key_name());
            }
            IndexImpactOnCompletion::Rename => {
                index.insert(cmd.get_key_name().clone(), cmd.get_key_type().clone());
                index.remove(execution_context.get_target());
            }
        }
        Ok(cmd)
    }

    fn is_index_command(&self, command: &str) -> bool {
        REDIS_INDEX_COMMANDS
            .iter()
            .any(|&cmd| cmd.eq_ignore_ascii_case(command))
    }

    fn build_index_command(&self, command: &Vec<String>) -> Result<CommandIdentifier, ParserError> {
        // support syntax: EXISTS name
        //                 DEL name
        //                 RENAME oldname newname

        if command.len() < 2 {
            return Err(ParserError::new(
                "Not enough identifiers provided for index command",
            ));
        }

        let command_type: RedisCommandType;
        let target: String;
        let action: String;
        let lock_type: LockType;
        let mut params: Vec<Bytes> = Vec::new();

        match command[0].to_uppercase().as_str() {
            "EXISTS" => {
                if command.len() != 2 {
                    return Err(ParserError::new(
                        "EXISTS command requires exactly one parameter",
                    ));
                }
                command_type = IndexCommand;
                action = "EXISTS".to_string();
                target = command[1].clone();
                // not no params for GET command
                lock_type = Read
            }
            "DEL" => {
                if command.len() != 2 {
                    return Err(ParserError::new("DEL command requires one parameter"));
                }
                command_type = IndexCommand;
                action = "DEL".to_string();
                target = command[1].clone();
                lock_type = Write
            }
            "RENAME" => {
                if command.len() != 3 {
                    return Err(ParserError::new("RENAME command requires two parameter"));
                }
                command_type = IndexCommand;
                action = "RENAME".to_string();
                target = command[1].clone();
                params.push(command[2].as_bytes().to_vec().into());
                lock_type = Write
            }
            _ => return Err(ParserError::new("Unsupported Index command type")),
        }

        Ok(CommandIdentifier::new(
            command_type,
            target,
            action,
            params,
            KeyType::Index,
            lock_type,
        ))
    }

    pub fn execute_index_command(
        &self,
        index: &mut MutexGuard<HashMap<String, KeyType>>,
        databases: &Arc<Databases>,
        command: &CommandIdentifier,
        original_key_type: &KeyType,
    ) -> Result<CommandCompleted, ExecutionError> {

        if command.get_action() ==  "EXISTS" {
            let response = if *original_key_type == Undefined { ":0\r\n".as_bytes().to_vec() } else { ":1\r\n".as_bytes().to_vec() };
            Ok(CommandCompleted::new(
                command.get_target(),
                KeyType::Index,
                NoImpact,
                Bytes::from(response),
            ))
        }
        else if command.get_action() == "DEL" {
            let mut num_deleted: u16 = 0;
            let impact: IndexImpactOnCompletion;
            if *original_key_type == Undefined {
                impact = NoImpact;
            }
            else { // TODO - is there a cleaner way to do this without the set of if statements for each type?
                if original_key_type == &KeyType::String {
                    // we know it has to be here
                    num_deleted = StringExecutor::delete(&databases.string, command.get_target());
                }
                if num_deleted == 0 {
                    impact = NoImpact;
                }
                else {
                    impact = Delete;
                }

            }
            let mut buf = BytesMut::new();
            buf.extend_from_slice(b":");
            buf.extend_from_slice(&num_deleted.to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            Ok(CommandCompleted::new(
                command.get_target(),
                original_key_type.clone(),
                impact,
               buf.freeze(),
            ))

        }
        else if command.get_action() == "RENAME" {
            if original_key_type == &KeyType::Undefined {
                Err(ExecutionError::new("-no such key"))?
            }
            let destination_key = std::str::from_utf8(&command.get_params()[0]).unwrap();
            // Delete the destination key if it exists
            let delete_command = self.build_index_command(&vec!["DEL".to_string(), destination_key.to_string()])?;
            self.internal_execute_command(&databases, &delete_command, index)?;

            if original_key_type == &KeyType::String {
                StringExecutor::rename(&databases.string, command.get_target(), destination_key);
            }
            Ok(CommandCompleted::new(
                destination_key,
                original_key_type.clone(),
                IndexImpactOnCompletion::Rename,
                Bytes::from("+OK\r\n"),
            ))
        }
        else {
            Err(ExecutionError::new(
                "-WRONGTYPE Operation against a key holding the wrong kind of value",
            ))
        }
    }

    fn contains(&self, key: &str) -> bool {
        self.shared.entries.lock().unwrap().contains_key(key)
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum KeyType {
    #[default]
    Undefined,
    Index, // Not really a 'type' but, the command is executing against the index
    String,
    Integer,
    List
}

#[derive(Debug)]
struct InternalStorage {
    entries: Mutex<HashMap<String, KeyType>>
}

impl InternalStorage {
    fn new() -> InternalStorage {
        InternalStorage {
            entries: Mutex::new(HashMap::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use bytes::Bytes;
    use crate::commands::ExecutionError;
    use crate::controller::Databases;
    use crate::index::{Index};
    use crate::string_executor::StringExecutor;

    #[test]
    fn given_unknown_command_return_error() {
        let index = Arc::new(Index::new());
            let databases = Arc::new(setup_databases());
            let request = vec!["UNKNOWN".to_string(), "key".to_string(), "value".to_string()];
            match Index::execute_command(&index, &databases, &request) {
                Ok(response) => {
                    panic!("Expected error, but got response: {:?}", response)
                },
                Err(error) => assert_eq!(error.get_message(), "Unknown Command")
            }
    }

   # [test]
   fn given_empty_index_when_get_then_key_not_added_to_index() {
        // Given an empty index
        let index = Arc::new(Index::new());
        let databases = Arc::new(setup_databases());
        let request = vec!["GET".to_string(), "key".to_string()]; // Note: GET does not change the index, nor fail if not found
        match Index::execute_command(&index, &databases, &request) {
            Ok(_) => {
                assert_eq!(index.contains("key"), false) // Note this test isn't interested in the return, only that the index isn't updated
            },
            Err(error) => panic!("Error executing command: {:?}", error)
        }

    }

    #[test]
    fn given_string_set_add_to_the_index() {
        // Given an empty index
        let index = Arc::new(Index::new());
        let databases = Arc::new(setup_databases());
        let response = set_a_string_value(&index, &databases, "key", "value");
        match response {
            Ok(_) => {
                assert_eq!(index.contains("key"), true)
            },
            Err(error) => panic!("Error executing command: {:?}", error)
        }
    }

    #[test]
    fn given_key_in_index_when_delete_key_is_removed() {
        let index = Arc::new(Index::new());
        let databases = Arc::new(setup_databases());
        set_a_string_value(&index, &databases, "key", "value").expect("Failed to setup Index for test");
        let request = vec!["DEL".to_string(), "key".to_string()];
        match Index::execute_command(&index, &databases, &request) {
            Ok(_) => {
                assert_eq!(index.contains("key"), false)
            },
            Err(error) => panic!("Error executing command: {:?}", error)
        }
        // now confirm the key was removed from the string database
        assert_eq!(databases.string.internal_exists("key"), false, "Key was not removed from the string database");
    }

    #[test]
    fn given_key_does_not_exist_when_delete_return_zero() {
        let index = Arc::new(Index::new());
        let databases = Arc::new(setup_databases());
        set_a_string_value(&index, &databases, "key", "value").expect("Failed to setup Index for test");
        let request = vec!["DEL".to_string(), "another_key".to_string()];
        match Index::execute_command(&index, &databases, &request) {
            Ok(response) => {
                assert_eq!(response, ":0\r\n")
            },
            Err(error) => panic!("Error executing command: {:?}", error)
        }
    }

    #[test]
    fn given_key_when_rename_and_dest_not_exists_name_has_changed() {
        const KEY_NAME: &'static str = "key";
        const NEW_KEY_NAME: &'static str = "new_key";

        let index = Arc::new(Index::new());
        let databases = Arc::new(setup_databases());
        set_a_string_value(&index, &databases, KEY_NAME, "value").expect("Failed to setup Index for test");
        let request = vec!["RENAME".to_string(), KEY_NAME.to_string(), NEW_KEY_NAME.to_string()];

        match Index::execute_command(&index, &databases, &request) {
            Ok(_) => {
                assert_eq!(index.contains(NEW_KEY_NAME), true);
                assert_eq!(index.contains(KEY_NAME), false)
            },
            Err(error) => panic!("Error executing command: {:?}", error)
        }
        // now confirm the key was removed from the string database
        assert_eq!(databases.string.internal_exists(KEY_NAME), false, "Key was not removed from the string database");
        assert_eq!(databases.string.internal_exists(NEW_KEY_NAME), true, "Key was not renamed from the string database");
    }

    #[test]
    fn given_key_which_already_exists_when_rename_delete_old_and_rename() {
        const KEY_NAME: &'static str = "key";
        const KEY_VALUE: &'static str = "value";
        const NEW_KEY_NAME: &'static str = "new_key";
        const NEW_KEY_VALUE: &'static str = "new_value";

        let index = Arc::new(Index::new());
        let databases = Arc::new(setup_databases());
        set_a_string_value(&index, &databases, KEY_NAME, KEY_VALUE).expect("Failed to setup Index for test");
        set_a_string_value(&index, &databases, NEW_KEY_NAME, NEW_KEY_VALUE).expect("Failed to setup Index for test");
        let request = vec!["RENAME".to_string(), KEY_NAME.to_string(), NEW_KEY_NAME.to_string()];

        match Index::execute_command(&index, &databases, &request) {
            Ok(_) => {
                assert_eq!(index.contains(NEW_KEY_NAME), true);
                assert_eq!(index.contains(KEY_NAME), false)
            },
            Err(error) => panic!("Error executing command: {:?}", error)
        }
        // now confirm the key was removed from the string database
        assert_eq!(databases.string.internal_exists(KEY_NAME), false, "Key was not removed from the string database");
        assert_eq!(databases.string.internal_exists(NEW_KEY_NAME), true, "Key was not renamed from the string database");

        // Finally, confirm that the value is the one initiatlly set
        let get_request = vec!["GET".to_string(), NEW_KEY_NAME.to_string()];
        match Index::execute_command(&index, &databases, &get_request) {
            Ok(get_value) => {
                assert_eq!(get_value, format!("+{}\r\n",KEY_VALUE).as_bytes());
            },
            Err(error) => panic!("Error executing command: {:?}", error)
        }
    }

    #[test]
    fn given_key_does_not_exist_when_rename_return_error() {
        const KEY_NAME: &'static str = "key";
        const NEW_KEY_NAME: &'static str = "new_key";
        let index = Arc::new(Index::new());
        let databases = Arc::new(setup_databases());
        let request = vec!["RENAME".to_string(), KEY_NAME.to_string(), NEW_KEY_NAME.to_string()];

        match Index::execute_command(&index, &databases, &request) {
            Ok(_) => {
                panic!("Expected error, but got response")
            },
            Err(error) => {
                assert_eq!(error.get_message(), "-no such key")
            }
        }
    }

    #[test]
    fn given_exists_command_for_existing_key_return_1() {
        let index = Arc::new(Index::new());
        let databases = Arc::new(setup_databases());
        set_a_string_value(&index, &databases, "key", "value").expect("Failed to setup Index for test");
        let request = vec!["EXISTS".to_string(), "key".to_string()];
        match Index::execute_command(&index, &databases, &request) {
            Ok(response) => {
                assert_eq!(response, b":1\r\n".as_ref())
            },
            Err(error) => panic!("Error executing command: {:?}", error)
        }
    }

    #[test]
    fn given_exists_command_for_nonexistent_key_return_0() {
        let index = Arc::new(Index::new());
        let databases = Arc::new(setup_databases());
        let request = vec!["EXISTS".to_string(), "nonexistent".to_string()];
        match Index::execute_command(&index, &databases, &request) {
            Ok(response) => {
                assert_eq!(response, b":0\r\n".as_ref())
            },
            Err(error) => panic!("Error executing command: {:?}", error)
        }
    }

    fn set_a_string_value(index: &Arc<Index>, databases: &Arc<Databases>, key: &str, value: &str) -> Result<Bytes, ExecutionError> {
        // common setup for all tests
        let request = vec!["SET".to_string(), key.to_string(), value.to_string()];
         Index::execute_command(&index, &databases, &request)
    }


    // TODO test - given a SET, followed by another command type, fail because the key exists as a string already

        fn setup_databases() -> Databases {
        Databases {
            string : Arc::new(StringExecutor::new())
        }
    }


    
}