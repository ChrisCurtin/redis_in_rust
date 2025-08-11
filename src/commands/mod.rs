use crate::{string_executor, tokenizer};

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

const UNKNOWN_COMMAND: &'static str = "Unknown command";

pub fn identify_command(request: &[u8]) -> Result<RedisCommand, ParserError> {
    let identifiers = tokenizer::identify_command(request)?;
    let command = &identifiers[0];
    if string_executor::is_string_command(command) {
         string_executor::build_string_command(command, &identifiers)
    } else {
         Err(ParserError::new(UNKNOWN_COMMAND))
    }

}

mod tests {
    use crate::commands::identify_command;

    #[test]
    fn given_get_string_then_return_get_command() {
        let request = b"*2\r\n$3\r\nGET\r\n$8\r\nMyString\r\n";
        let command = identify_command(request);
        match command {
            Ok(cmd) => {
                assert_eq!(cmd.get_action(), "GET");
                assert_eq!(cmd.get_target(), "MyString");
                assert_eq!(cmd.get_params().len(), 0);
            },
            Err(e) => panic!("Expected command, got error: {}", e.get_message()),
        }
    }

    #[test]
    fn given_set_string_then_return_set_command() {
        let request = b"*3\r\n$3\r\nSET\r\n$8\r\nMyString\r\n$5\r\nValue\r\n";
        let command = identify_command(request);
        match command {
            Ok(cmd) => {
                assert_eq!(cmd.get_action(), "SET");
                assert_eq!(cmd.get_target(), "MyString");
                assert_eq!(cmd.get_params().len(), 1);
                assert_eq!(cmd.get_params()[0], "Value");
            },
            Err(e) => panic!("Expected command, got error: {}", e.get_message()),
        }
    }

}