#[derive(Debug)]
pub struct RedisCommand {
    command_type: String,
    target: String,
    action: String,
    params: Vec<String>,
}

#[derive(Debug)]
pub struct ParserError {
    message: String,
}

pub mod command {
    use super::RedisCommand;
    use super::ParserError;

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

    impl RedisCommand {
        pub fn new(command_type: String, target: String, action: String, params: Vec<String>) -> Self {
            RedisCommand {
                command_type,
                target,
                action,
                params,
            }
        }

        pub fn to_string(&self) -> String {
            let params_str = self.params.join(" ");
            format!("{} {} {} {}", self.command_type, self.target, self.action, params_str)
        }


        pub fn get_command_type(&self) -> &str {
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
}