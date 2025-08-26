// List of all the keys, and their types

use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use bytes::Bytes;
use crate::commands::{ExecutionError};
use crate::controller::Databases;
use crate::index::RedisCommandType::{UnknownCommand, StringCommand};
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
    Delete
}

#[derive(Debug, PartialEq)]
pub enum RedisCommandType {
    UnknownCommand,
    StringCommand
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

pub fn is_index_command(command: &str) -> bool {
    REDIS_INDEX_COMMANDS
        .iter()
        .any(|&cmd| cmd.eq_ignore_ascii_case(command))
}


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
    pub fn contains(&self, key: &str) -> bool {
        self.shared.entries.lock().unwrap().contains_key(key)
    }

    pub fn execute_command(&self, databases: &Arc<Databases>, request: &Vec<String>) -> Result<Bytes, ExecutionError> {
        let command = &request[0];
        let execution_context =
            if (StringExecutor::is_command_supported(&command)) {
                StringExecutor::build_command(&request)?
            } else {
                Err(ExecutionError::new("Unknown Command"))?
            };

        // lock the index
        {
            let mut index = self.shared.entries.lock().unwrap();
            // See if the key exists in the index, then check that the types match
            //
            let key = execution_context.get_target();
            if index.contains_key(key) {
                let key_type = index.get(key).unwrap();
                if (key_type != execution_context.get_key_type()) {
                    return Err(ExecutionError::new("Key already exists with different type"))
                }
            }

            let command_result: Result<CommandCompleted, ExecutionError> =
                match execution_context.get_command_type() {
                    UnknownCommand => { Ok(CommandCompleted::default()) } // We should never get here, but we need the case to be certain all the RedisCommandTypes are covered
                    StringCommand => {
                        StringExecutor::execute_string_command(&databases.string, &execution_context)
                    }
                };

            let cmd = command_result?;
            match cmd.get_impact_on_index() {
                IndexImpactOnCompletion::NoImpact => {}
                IndexImpactOnCompletion::Add => {
                    index.insert(cmd.get_key_name().clone(), cmd.get_key_type().clone());
                }
                IndexImpactOnCompletion::Delete => {
                    index.remove(cmd.get_key_name());
                }
            }
            Ok(cmd.get_response().clone())
        } // we unlock when we leave the block
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum KeyType {
    #[default]
    Undefined,
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
    use crate::controller::Databases;
    use crate::index::{Index, KeyType};
    use crate::string_executor::StringExecutor;
    // Test - given an uknnown caommand, error

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
                // TODO - how brittle is this? Would it pass if the GET wasn't a string command?
            },
            Err(error) => panic!("Error executing command: {:?}", error)
        }

    }
    // test - given set string command, add to the index
    // test - given get string command without being in index already, no change to index,
    // test - given del command, remove from index
    // test - given a SET, followed by another command type, fail because the key exists

    // #[test]
    // fn given_set_command_when_executed_then_key_is_added() {
    //     // Given an empty index
    //     let index = Arc::new(Index::new());
    //     let databases = Arc::new(setup_databases());
    //     let request = vec!["SET".to_string(), "key".to_string(), "value".to_string()];
    //
    //     // when a SET String command is received, add to the String database and add to the index
    //     match Index::execute_command(&index, &databases, &request) {
    //         Ok(response) => {
    //             // TODO - get the index to see if the key was added and is of the correct type
    //             assert_eq!(response, "OK".as_bytes())
    //         },
    //         Err(error) => panic!("Error executing command: {:?}", error)
    //     }
    // }

    fn setup_databases() -> Databases {
        Databases {
            string : Arc::new(StringExecutor::new())
        }
    }






    // #[test]
    // fn given_empty_index_when_adding_key_then_key_is_added() {
    //  let  index = Index::new();
    //     index.add( "key".to_string(), KeyType::String);
    //     let key_type = index.get("key".to_string());
    //     assert_eq!(key_type, KeyType::String);
    // }
    
}