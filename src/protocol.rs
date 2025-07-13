
const EMPTY_REQUEST: &str = "Request is empty";
const NO_TOKENS_FOUND: &str = "No tokens found in the request";
const INVALID_REQUEST_STRUCTURE: &str = "Invalid request structure, expected an array indicator '*' at the start";
const INVALID_TOKEN_FORMAT: &str = "Invalid token format, expected newline after carriage return";
const EMPTY_TOKEN_VALUE: & str = "Empty token value; expected at least one character before carriage return";

pub mod protocol {
    use crate::command::{ParserError, RedisCommand};
    use crate::protocol::EMPTY_REQUEST;
    use crate::protocol::INVALID_TOKEN_FORMAT;
    use crate::protocol::NO_TOKENS_FOUND;
    use crate::protocol::INVALID_REQUEST_STRUCTURE;
    use crate::protocol::EMPTY_TOKEN_VALUE;

    #[derive(Debug, PartialEq)]
    enum SyntaxElement {
        Array { size: u32 },
        String { value: String },
        Integer { value: i64 },
        Null,
        Boolean { value: bool },
        Double { value: f64 }
    }

    struct Token {
        value: Vec<u8>,
        size: usize
    }

    pub fn parse_request(request: &[u8]) -> Result<RedisCommand, ParserError> {
        if request.is_empty() {
            return Err(ParserError::new(EMPTY_REQUEST));
        }
        let tokens = match tokenize_request(request) {
            Ok(tokens) => tokens,
            Err(e) => return Err(ParserError::new(e)),
        };
        validate_request_structure(&tokens)?;


        // Here you would typically parse the tokens into a RedisCommand
        // For now, we will just return a placeholder command
        Ok(RedisCommand::new(
            "SET".to_string(),
            "key".to_string(),
            "value".to_string(),
            vec![],
        ))
    }

    fn validate_request_structure(tokens: &[Token]) -> Result<(), ParserError> {
        if tokens.is_empty() {
            return Err(ParserError::new(NO_TOKENS_FOUND));
        }
        if tokens[0].value.is_empty() || tokens[0].value[0] != b'*' {
            return Err(ParserError::new(INVALID_REQUEST_STRUCTURE));
        }

        Ok(())
    }

    fn tokenize_request(request: &[u8]) -> Result<Vec<Token>, &str> {
        let mut tokens = Vec::new();
        let mut start = 0;

        while start < request.len() {
            match get_token(request, start) {
                Ok(token) => {
                    start += token.size;
                    tokens.push(token);
                },
                Err(e) => return Err(e),
            }
        }
        Ok(tokens)
    }



    fn get_token(input: &[u8], start: usize) -> Result<Token, &str> {
        if input.is_empty() || start >= input.len() {
            return Err(EMPTY_REQUEST);
        }
        let mut count_of_characters = 0;
        for index in start..input.len() {
            let byte = input[index];
            if byte == b'\r' {
                if count_of_characters == 0 {
                    return Err(EMPTY_TOKEN_VALUE);
                }
                if input[start + count_of_characters + 1] as char != '\n' {
                    return Err(INVALID_TOKEN_FORMAT);
                }
                break;
            }
            count_of_characters += 1;
        }
        Ok(Token {
            value: input[start..start + count_of_characters].to_vec(),
            size: count_of_characters + 2 // +2 for \r\n
        })
    }


    // fn parse_bulk_string(tokens: Vec<Token>, start: usize) -> Result<(SyntaxElement, usize), String> {
    //     let mut token_index = start;
    //     while token_index < tokens.len() {
    //         let token = &tokens[start];
    //         let token_type = token.value.chars(0
    //     }
    //
    //    return Err("Not implemented yet")
    //
    // }

    #[cfg(test)]
    mod tests {
        use crate::protocol::*;


        #[test]
        fn given_empty_request_when_parse_request_then_returns_error() {
            let request: &[u8] = b"";
            let command = protocol::parse_request(request);
            match command {
                Ok(_) => panic!("Expected error, got command"),
                Err(e) => assert_eq!(e.get_message(), EMPTY_REQUEST),
            }
        }

        #[test]
        fn given_missing_array_indicator_when_parse_request_then_returns_error() {
            let request = b"$2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";  // Missing the initial '*'
            let command = protocol::parse_request(request);
            match command {
                Ok(_) => panic!("Expected error, got command"),
                Err(e) => assert_eq!(e.get_message(), INVALID_REQUEST_STRUCTURE),
            }
        }



        // NEED TO FIX - everything below here is a scratch code
        // #[test]
        // fn given_valid_request_when_parse_request_then_returns_command() {
        //     let request = b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
        //     let command = protocol::parse_request(request);
        //     match command {
        //         Ok(cmd) => {
        //             assert_eq!(cmd.get_command_type(), "SET");
        //             assert_eq!(cmd.get_target(), "key1");
        //             assert_eq!(cmd.get_action(), "value1");
        //             assert!(cmd.get_params().is_empty());
        //         },
        //         Err(e) => panic!("Expected command, got error: {}", e),
        //     };
        // }
        #[test]
        fn test_get_token() {
            let input = b"$3\r\nSET\r\n";
            let result = protocol::get_token(input, 0);
            assert!(result.is_ok());
            let token = result.unwrap();
            assert_eq!(String::from_utf8(token.value.to_vec()).unwrap(), "$3");
            assert_eq!(token.size, 4); // $3\r\n
        }

        #[test]
        fn test_get_token_empty() {
            let input: &[u8] = b"";
            let result = protocol::get_token(input, 0);
            assert!(result.is_err());
            assert_eq!(result.err(), Some(EMPTY_REQUEST));
        }

        #[test]
        fn test_multiple_tokens() {
            let input = b"$3\r\nSET\r\n$5\r\nkey1\r\n$5\r\nvalue1\r\n";
            let tokens = protocol::tokenize_request(input).unwrap();
            assert_eq!(tokens.len(), 6);

            assert_eq!(String::from_utf8(tokens[0].value.to_vec()).unwrap(), "$3");
            assert_eq!(String::from_utf8(tokens[1].value.to_vec()).unwrap(), "SET");
            assert_eq!(String::from_utf8(tokens[2].value.to_vec()).unwrap(), "$5");
            assert_eq!(String::from_utf8(tokens[3].value.to_vec()).unwrap(), "key1");
            assert_eq!(String::from_utf8( tokens[4].value.to_vec()).unwrap(), "$5");
            assert_eq!(String::from_utf8( tokens[5].value.to_vec()).unwrap(), "value1");
        }
    }
}