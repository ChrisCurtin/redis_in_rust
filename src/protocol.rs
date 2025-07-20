
const EMPTY_REQUEST: &str = "Request is empty";
const NO_TOKENS_FOUND: &str = "No tokens found in the request";
const INVALID_REQUEST_STRUCTURE: &str = "Invalid request structure, expected an array indicator '*' at the start";
const INVALID_TOKEN_FORMAT: &str = "Invalid token format, expected newline after carriage return";
const EMPTY_TOKEN_VALUE: & str = "Empty token value; expected at least one character before carriage return";
const TOKEN_SIZE_NOT_A_BYTE: &'static str = "Unable to determine size of Token";
const TOKEN_SIZE_NOT_A_NUMBER: &'static str = "Token size is not a valid number";
const SIZE_CANNOT_BE_ZERO: &'static str = "Array size cannot be zero";
const IDENTIFIER_IS_WRONG_SIZE: &'static str = "Identifier size is less than expected";

pub mod protocol {
    use crate::commands::{ParserError, RedisCommand};
    use crate::protocol::{EMPTY_REQUEST, TOKEN_SIZE_NOT_A_NUMBER, TOKEN_SIZE_NOT_A_BYTE, SIZE_CANNOT_BE_ZERO, IDENTIFIER_IS_WRONG_SIZE};
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

    #[derive(Debug)]
    pub enum Identifier {
        String(String),
        Integer(i64),
    }

    pub struct IdentifierType {
        identifier: Identifier,
        consumed_tokens: usize, // number of tokens
    }

    impl IdentifierType {
        pub fn get_identifier(&self) -> &Identifier {
            &self.identifier
        }
    }

    fn determine_command(tokens: &[Token], start: usize) -> Result<RedisCommand, ParserError> {
        if start >= tokens.len() {
            return Err(ParserError::new(NO_TOKENS_FOUND));
        }
        Err(ParserError::new(NO_TOKENS_FOUND)) // TODO: Implement this function
    }

    // starting at 1, (0 based), get the token
    // parse the token to determine if aggregate or simple & size
    // if simple get the next token, and confirm the size
    // if the size matches build a Type with a single value (use a Rust enum here? vs. something generic?) return Type and cout of consumed tokens
    //
    // if aggregate, using count of consumer token, call recursively
    // when return, determine the command and create the Command struct
    fn determine_identifiers(tokens: &[Token], start: usize) -> Result<IdentifierType, ParserError> {
        if start >= tokens.len() {
            return Err(ParserError::new(NO_TOKENS_FOUND));
        }
        let mut index: usize = start;
        let first_token = &tokens[index];
        let mut index = index + 1;

        return match first_token.value.first() {
            Some(&b'$') => { // bulk string
                let num_elements = get_number_of_chars(first_token)?;
                Ok(IdentifierType {
                    identifier: convert_to_string_identifier(&tokens[index], num_elements)?,
                    consumed_tokens: 2, // We consumed two tokens
                })
            }

            Some(&b':') => {
                // Integer values don't have a size, so we can parse it directly

                let integer_value = String::from_utf8(first_token.value[1..].to_vec())
                    .map_err(|_| ParserError::new(TOKEN_SIZE_NOT_A_BYTE))?
                    .parse::<i64>()
                    .map_err(|_| ParserError::new(TOKEN_SIZE_NOT_A_NUMBER))?;

                Ok(IdentifierType {
                    identifier: Identifier::Integer(integer_value),
                    consumed_tokens: 1, // We consumed the first token
                })
            }

            Some(&b'*') => {
                // Array, which means this is a complex command
                let array = process_array_command(first_token, tokens, index);

            }

            _ => {
                // This is a simple command, parse it accordingly
                let command_type = String::from_utf8(first_token.value.clone())
                    .map_err(|_| ParserError::new(INVALID_TOKEN_FORMAT))?;

                // TODO - this is only here until we add all the data types and then we'd fail here
                return Ok(IdentifierType {
                    identifier: convert_to_string_identifier(first_token, 1)?,
                    consumed_tokens: 1, // We consumed the first token
                });
            }
        }
    }

    fn process_array_command(first_token: &Token, tokens: &[Token], start: usize) -> Result<IdentifierType, ParserError> {
        // This is an array command, get the number of elements
        let num_elements:usize = get_number_of_chars(first_token)?;
        let mut identifiers: Vec<Identifier> = Vec::new();
        let mut index = start;
        for array_element in 0..num_elements {
            let id = determine_identifiers(tokens, index+array_element)?;
            identifiers.push(id.identifier);
            index += id.consumed_tokens; // Move the index forward by the number of tokens consumed
        }

       // Now figure out what the command is and what is it doing

        Ok(IdentifierType {
            identifier: convert_to_string_identifier(first_token, num_elements)?,
            consumed_tokens: 1, // We consumed the first token
        })
    }

    fn get_number_of_chars(token: &Token) -> Result<usize, ParserError> {
        let num_elements_str = String::from_utf8(token.value[1..].to_vec())
            .map_err(|_| ParserError::new(TOKEN_SIZE_NOT_A_BYTE))?;
        let size = num_elements_str.parse::<usize>()
            .map_err(|_| ParserError::new(TOKEN_SIZE_NOT_A_NUMBER))?;
        if size == 0 {
            return Err(ParserError::new(SIZE_CANNOT_BE_ZERO));
        }
        Ok(size)
    }

    fn convert_to_string_identifier(token: &Token, num_chars:usize) -> Result<Identifier, ParserError> {
        let offset: usize = 1 + num_chars;
        if token.size < offset  { // the protocol said this token should be num_chars long
            return Err(ParserError::new(IDENTIFIER_IS_WRONG_SIZE));
        }
        let value_str = String::from_utf8(token.value.to_vec())
            .map_err(|_| ParserError::new(TOKEN_SIZE_NOT_A_BYTE))?;
             Ok(Identifier::String(value_str))
    }

    #[cfg(test)]
    mod tests {
        use crate::protocol::*;
        use crate::protocol::protocol::{Identifier, Token};

        // Token parsing tests TODO - fix the module structure
        #[test]
        fn given_byte_array_when_asked_return_integer_value() {
            let input = b"*22";
            let token = Token{
                value: input.to_vec(),
                size: input.len(),
            };

            let result = protocol::get_number_of_chars(&token);
            match result {
                Ok(num) => assert_eq!(num, 22),
                Err(e) => panic!("Expected number, got error: {}", e.get_message()),
            }

        }

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

        #[test]
        fn given_string_input_when_parse_then_return_identifier() {
            let input = b"$3\r\nSET\r\n";
            let tokens = protocol::tokenize_request(input);
            if let Ok(tokens) = tokens {
                let identifiers = protocol::determine_identifiers(&tokens, 0);
                match (identifiers) {
                    Ok(identifier_type) => {
                        if let Identifier::String(value) = identifier_type.identifier {
                            assert_eq!(value, "SET");
                            assert_eq!(identifier_type.consumed_tokens, 2);
                        } else {
                            panic!("Expected String identifier, got {:?}", identifier_type.identifier);
                        }
                    },
                    Err(e) => panic!("Expected identifier, got error: {}", e.get_message()),
                }
            }
        }

        #[test]
        fn given_integer_input_when_parse_then_return_identifier() {
            let input = b":42\r\n";
            let tokens = protocol::tokenize_request(input);
            if let Ok(tokens) = tokens {
                let identifiers = protocol::determine_identifiers(&tokens, 0);
                match (identifiers) {
                    Ok(identifier_type) => {
                        if let Identifier::Integer(value) = identifier_type.identifier {
                            assert_eq!(value, 42);
                            assert_eq!(identifier_type.consumed_tokens, 1);
                        } else {
                            panic!("Expected Integer identifier, got {:?}", identifier_type.identifier);
                        }
                    },
                    Err(e) => panic!("Expected identifier, got error: {}", e.get_message()),
                }
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