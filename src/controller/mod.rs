use app_properties::AppProperties;
use crate::thread_pool::ThreadPool;
use crate::{string_executor, tokenizer};
use std::{io, io::{prelude::*}, net::{TcpListener, TcpStream}, sync::Arc};
use crate::commands::{ExecutionError, ParserError};
use crate::string_executor::{build_string_command, execute_string_command, string_storage};

const HOME: &'static str = "127.0.0.1";
const DEFAULT_PORT: u16 = 6379;
const DEFAULT_THREAD_POOL_SIZE: usize = 4;

pub fn initialize_controller() {
    let properties = AppProperties::new();
    let mut server_address = properties.get("server.host");
    let server_port = properties.get("server.port").parse::<u16>().unwrap_or(DEFAULT_PORT);
    let thread_pool_size = properties.get("thread.pool.size").parse::<usize>().unwrap_or(DEFAULT_THREAD_POOL_SIZE);
    if server_address.is_empty() {
        server_address = HOME;
    }
    println!("Starting server at {}:{}", server_address, server_port);

    let listener = TcpListener::bind((server_address, server_port)).unwrap();
    let pool = ThreadPool::new(thread_pool_size);

    let string_db = Arc::new(string_storage::StringStorage::new());

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let string_db = Arc::clone(&string_db);

        pool.execute(move || {
            handle_connection(stream, &string_db);
        });
    }

    println!("Shutting down.");
    
}
fn handle_connection(mut stream: TcpStream, string_db: &Arc<string_storage::StringStorage>) {

    // ./redli -h localhost -p 6379 --debug

    loop {
        // Wrap the stream in a BufReader, so we can use the BufRead methods
        let mut reader = io::BufReader::new(&mut stream);

        // Read current data in the TcpStream
        let received = reader.fill_buf();
        match received {
            Ok(received) => {
                println!("Raw bytes: {:?}", received);
                let size = received.len();
                let command = tokenizer::identify_command(received);
                reader.consume(size);
                match command {
                    Ok(request) => {
                        println!("Received Request: {:?}", request);
                        if string_executor::is_string_command(&request[0]) {
                            let command = build_string_command(&request);
                            match command {
                                Ok(cmd) => { // TODO - refactor this. these nested error handling is not good
                                    match execute_string_command(&string_db, &cmd) {
                                        Ok(result) => {
                                            println!("Result: {:?}", result);
                                            stream.write_all(result.iter().as_slice()).unwrap()
                                        },
                                        Err(error) => {
                                            println!("Error: {:?}", error);
                                            stream.write_all(format_execution_error(&error).as_slice()).unwrap();
                                        }
                                    }
                                },
                                Err(error) => {
                                    println!("Parse Error 2: {:?}", error);
                                    stream.write_all(format_parse_error(&error).as_slice()).unwrap()
                                }
                            };
                        }
                        else {
                            let message = format!("-ERR Unknown Command {} \r\n", &request[0]);
                            stream.write_all(format_error(&message).as_slice()).unwrap();
                        }
                    },
                    Err(error) => {
                        println!("Parse Error: {:?}", error);
                        stream.write_all(format_parse_error(&error).as_slice()).unwrap();
                    }
                }
            },
            Err(msg) => {
                println!("System Error: {:?}", msg);
               return // issue with the TCP stream so close it and exit this thread
            }
        };

    }
}

fn format_parse_error(error:&ParserError) -> Vec<u8> {
    format_error(error.get_message())
}

fn format_execution_error(error:&ExecutionError) -> Vec<u8> {
    format_error(error.get_message())
}

fn format_error(error:&str) -> Vec<u8> {
    println!("Error {:?}", error);
    format!("-ERR {} \r\n", error).as_bytes().to_vec()
}
