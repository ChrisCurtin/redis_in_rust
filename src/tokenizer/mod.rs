
use crate::commands::ParserError;

const EMPTY_REQUEST: &str = "Request is empty";
const NO_TOKENS_FOUND: &str = "No tokens found in the request";
const INVALID_REQUEST_STRUCTURE: &str =
    "Invalid request structure, expected an array indicator '*' at the start";
const INVALID_TOKEN_FORMAT: &str = "Invalid token format, expected newline after carriage return";
const EMPTY_TOKEN_VALUE: &str =
    "Empty token value; expected at least one character before carriage return";
const TOKEN_SIZE_NOT_A_BYTE: &'static str = "Unable to determine size of Token";
const TOKEN_SIZE_NOT_A_NUMBER: &'static str = "Token size is not a valid number";
const SIZE_CANNOT_BE_ZERO: &'static str = "Array size cannot be zero";
const IDENTIFIER_IS_WRONG_SIZE: &'static str = "Identifier size is less than expected";

const TOKEN_IS_NOT_VALID_UTF8: &'static str = "Identifiers are not valid UTF-8 bytes";
const INVALID_NO_SIZE_TOKEN: &'static str = "Expected size token '$' before identifier";
const INVALID_NO_IDENTIFIER: &'static str = "Expected identifier after size token";
const INVALID_REQUEST_INCORRECT_SIZE: &'static str =
    "Invalid structure, number of identifiers does not match expected size";
struct Token {
    value: Vec<u8>,
    size: usize,
}
pub fn identify_command(request: &[u8]) -> Result<Vec<String>, ParserError> {
    if request.is_empty() {
        return Err(ParserError::new(EMPTY_REQUEST));
    }
    let tokens = match tokenize_request(request) {
        Ok(tokens) => tokens,
        Err(e) => return Err(ParserError::new(e)),
    };
    let response = validate_request_structure(&tokens)?;
    Ok(response)
}

fn validate_request_structure(tokens: &[Token]) -> Result<Vec<String>, ParserError> {
    if tokens.is_empty() {
        return Err(ParserError::new(NO_TOKENS_FOUND));
    }
    if tokens[0].value.is_empty() || tokens[0].value[0] != b'*' {
        return Err(ParserError::new(INVALID_REQUEST_STRUCTURE));
    }
    let mut response: Vec<String> = Vec::new();
    let num_children = get_number_of_chars(&tokens[0])?;

    for index in (1..tokens.len()).step_by(2) {
        if tokens[index].value[0] != b'$' {
            return Err(ParserError::new(INVALID_NO_SIZE_TOKEN));
        }
        let size = get_number_of_chars(&tokens[index])?;
        if index + 1 >= tokens.len() {
            return Err(ParserError::new(INVALID_NO_IDENTIFIER));
        }
        let identifier = String::from_utf8(tokens[index + 1].value[0..].to_vec())
            .map_err(|_| ParserError::new(TOKEN_IS_NOT_VALID_UTF8))?;
        if identifier.is_empty() || identifier.len() != size {
            return Err(ParserError::new(IDENTIFIER_IS_WRONG_SIZE));
        }
        response.push(identifier);
    }
    // validate the number of identifiers matches the expected array size
    if response.len() != num_children {
        return Err(ParserError::new(INVALID_REQUEST_INCORRECT_SIZE));
    }

    Ok(response)
}

fn get_number_of_chars(token: &Token) -> Result<usize, ParserError> {
    let num_elements_str = String::from_utf8(token.value[1..].to_vec())
        .map_err(|_| ParserError::new(TOKEN_SIZE_NOT_A_BYTE))?;
    let size = num_elements_str
        .parse::<usize>()
        .map_err(|_| ParserError::new(TOKEN_SIZE_NOT_A_NUMBER))?;
    if size == 0 {
        return Err(ParserError::new(SIZE_CANNOT_BE_ZERO));
    }
    Ok(size)
}

fn tokenize_request(request: &[u8]) -> Result<Vec<Token>, &str> {
    let mut tokens = Vec::new();
    let mut start = 0;

    while start < request.len() {
        match get_token(request, start) {
            Ok(token) => {
                start += token.size;
                tokens.push(token);
            }
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
            if index + 1 >= input.len() || input[index + 1] != b'\n' {
                return Err(INVALID_TOKEN_FORMAT); // TODO - make sure we have a test case for this
            }
            break;
        }
        count_of_characters += 1;
    }
    Ok(Token {
        value: input[start..start + count_of_characters].to_vec(),
        size: count_of_characters + 2, // +2 for \r\n
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::{EMPTY_REQUEST, INVALID_REQUEST_STRUCTURE};

    #[test]
    fn given_empty_request_when_parse_request_then_returns_error() {
        let request: &[u8] = b"";
        let command = identify_command(request);
        match command {
            Ok(_) => panic!("Expected error, got command"),
            Err(e) => assert_eq!(e.get_message(), EMPTY_REQUEST),
        }
    }

    #[test]
    fn given_missing_array_indicator_when_parse_request_then_returns_error() {
        let request = b"$2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"; // Missing the initial '*'
        let command = identify_command(request);
        match command {
            Ok(_) => panic!("Expected error, got command"),
            Err(e) => assert_eq!(e.get_message(), INVALID_REQUEST_STRUCTURE),
        }
    }

    #[test]
    fn given_byte_array_when_asked_return_integer_value() {
        let input = b"*22";
        let token = Token {
            value: input.to_vec(),
            size: input.len(),
        };

        let result = get_number_of_chars(&token);
        match result {
            Ok(num) => assert_eq!(num, 22),
            Err(e) => panic!("Expected number, got error: {}", e.get_message()),
        }
    }

    #[test]
    fn test_get_token() {
        let input = b"$3\r\nSET\r\n";
        let result = get_token(input, 0);
        assert!(result.is_ok());
        let token = result.unwrap();
        assert_eq!(String::from_utf8(token.value.to_vec()).unwrap(), "$3");
        assert_eq!(token.size, 4); // $3\r\n
    }

    #[test]
    fn test_get_token_empty() {
        let input: &[u8] = b"";
        let result = get_token(input, 0);
        assert!(result.is_err());
        assert_eq!(result.err(), Some(EMPTY_REQUEST));
    }

    #[test]
    fn test_multiple_tokens() {
        let input = b"$3\r\nSET\r\n$5\r\nkey1\r\n$5\r\nvalue1\r\n";
        let tokens = tokenize_request(input).unwrap();
        assert_eq!(tokens.len(), 6);

        assert_eq!(String::from_utf8(tokens[0].value.to_vec()).unwrap(), "$3");
        assert_eq!(String::from_utf8(tokens[1].value.to_vec()).unwrap(), "SET");
        assert_eq!(String::from_utf8(tokens[2].value.to_vec()).unwrap(), "$5");
        assert_eq!(String::from_utf8(tokens[3].value.to_vec()).unwrap(), "key1");
        assert_eq!(String::from_utf8(tokens[4].value.to_vec()).unwrap(), "$5");
        assert_eq!(
            String::from_utf8(tokens[5].value.to_vec()).unwrap(),
            "value1"
        );
    }

    #[test]
    fn test_validate_request_structure_empty_request() {
        let tokens: Vec<Token> = vec![];
        let result = validate_request_structure(&tokens);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().get_message(), NO_TOKENS_FOUND);
    }

    #[test]
    fn test_validate_request_structure_no_leading_star() {
        let tokens = vec![Token {
            value: b"$2".to_vec(),
            size: 2,
        }];
        let result = validate_request_structure(&tokens);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().get_message(),
            INVALID_REQUEST_STRUCTURE
        );
    }

    #[test]
    fn test_validate_request_structure_no_dollar_before_identifier() {
        let tokens = vec![
            Token {
                value: b"*1".to_vec(),
                size: 2,
            },
            Token {
                value: b"SET".to_vec(),
                size: 3,
            }, // Should be $3
        ];
        let result = validate_request_structure(&tokens);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().get_message(), INVALID_NO_SIZE_TOKEN);
    }

    #[test]
    fn test_validate_request_structure_no_identifier_after_dollar() {
        let tokens = vec![
            Token {
                value: b"*1".to_vec(),
                size: 2,
            },
            Token {
                value: b"$3".to_vec(),
                size: 2,
            },
            // Missing identifier token
        ];
        let result = validate_request_structure(&tokens);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().get_message(), INVALID_NO_IDENTIFIER);
    }

    #[test]
    fn test_validate_request_structure_identifier_wrong_size() {
        let tokens = vec![
            Token {
                value: b"*1".to_vec(),
                size: 2,
            },
            Token {
                value: b"$4".to_vec(),
                size: 2,
            },
            Token {
                value: b"SET".to_vec(),
                size: 3,
            }, // Should be 4 bytes
        ];
        let result = validate_request_structure(&tokens);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().get_message(),
            IDENTIFIER_IS_WRONG_SIZE
        );
    }

    #[test]
    fn test_validate_request_structure_identifier_count_mismatch() {
        let tokens = vec![
            Token {
                value: b"*2".to_vec(),
                size: 2,
            },
            Token {
                value: b"$3".to_vec(),
                size: 2,
            },
            Token {
                value: b"SET".to_vec(),
                size: 3,
            },
        ];
        let result = validate_request_structure(&tokens);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().get_message(),
            INVALID_REQUEST_INCORRECT_SIZE
        );
    }

    #[test]
    fn test_validate_request_structure_valid_request() {
        let tokens = vec![
            Token {
                value: b"*2".to_vec(),
                size: 2,
            },
            Token {
                value: b"$3".to_vec(),
                size: 2,
            },
            Token {
                value: b"SET".to_vec(),
                size: 3,
            },
            Token {
                value: b"$4".to_vec(),
                size: 2,
            },
            Token {
                value: b"key1".to_vec(),
                size: 4,
            },
        ];
        let result = validate_request_structure(&tokens);
        match result {
            Ok(identifiers) => {
                assert_eq!(identifiers.len(), 2);
                assert_eq!(identifiers[0], "SET");
                assert_eq!(identifiers[1], "key1");
            }
            Err(e) => panic!("Expected valid identifiers, got error: {}", e.get_message()),
        }
    }
}
