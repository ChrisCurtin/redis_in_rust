use crate::commands::{ExecutionError, ParserError};
use crate::index::Index;
use crate::string_executor::StringExecutor;
use crate::thread_pool::ThreadPool;
use crate::tokenizer;
use app_properties::AppProperties;
use std::{
    io,
    io::prelude::*,
    net::{TcpListener, TcpStream},
    sync::Arc,
};

const HOME: &'static str = "127.0.0.1";
const DEFAULT_PORT: u16 = 6379;
const DEFAULT_THREAD_POOL_SIZE: usize = 4;

pub struct Databases {
    pub string: Arc<StringExecutor>,
}

pub fn initialize_controller() {
    let properties = AppProperties::new();
    let mut server_address = properties.get("server.host");
    let server_port = properties
        .get("server.port")
        .parse::<u16>()
        .unwrap_or(DEFAULT_PORT);
    let thread_pool_size = properties
        .get("thread.pool.size")
        .parse::<usize>()
        .unwrap_or(DEFAULT_THREAD_POOL_SIZE);
    if server_address.is_empty() {
        server_address = HOME;
    }
    log::info!("Starting server at {}:{}", server_address, server_port);

    let listener = TcpListener::bind((server_address, server_port)).unwrap();
    let pool = ThreadPool::new(thread_pool_size);

    // The set of all the keys in the database, with the data type
    let index_db = Arc::new(Index::new());

    let databases = Arc::new(Databases {
        string: Arc::new(StringExecutor::new()),
    });

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let databases = Arc::clone(&databases);
        let index_db = Arc::clone(&index_db);

        pool.execute(move || {
            handle_connection(stream, &index_db, &databases);
        });
    }

    log::info!("Shutting down.");
}

fn handle_connection(mut stream: TcpStream, index: &Arc<Index>, databases: &Arc<Databases>) {
    loop {
        // Wrap the stream in a BufReader, so we can use the BufRead methods
        let mut reader = io::BufReader::new(&mut stream);

        // Read current data in the TcpStream
        let received = reader.fill_buf();
        match received {
            Ok(received) => {
                log::debug!("Raw bytes: {:?}", received);
                let size = received.len();
                if size == 0 {
                    return;
                } // the connection was closed, so exit this thread

                // Identify the command
                let command = tokenizer::identify_command(received);
                reader.consume(size);

                match command {
                    Ok(request) => {
                        log::info!("Received Request: {:?}", request);

                        match index.execute_command(&databases, &request) {
                            Ok(result) => {
                                log::debug!("Result: {:?}", result);
                                stream.write_all(result.iter().as_slice()).unwrap()
                            }
                            Err(error) => {
                                log::error!("Error: {:?}", error);
                                stream
                                    .write_all(format_execution_error(&error).as_slice())
                                    .unwrap();
                            }
                        }
                    }
                    Err(error) => {
                        log::error!("Parse Error: {:?}", error);
                        stream
                            .write_all(format_parse_error(&error).as_slice())
                            .unwrap();
                    }
                }
            }
            Err(msg) => {
                log::error!("System Error: {:?}", msg);
                return; // issue with the TCP stream so close it and exit this thread
            }
        };
    }
}

fn format_parse_error(error: &ParserError) -> Vec<u8> {
    format_error(error.get_message())
}

fn format_execution_error(error: &ExecutionError) -> Vec<u8> {
    format_error(error.get_message())
}

fn format_error(error: &str) -> Vec<u8> {
    log::info!("Error {:?}", error);
    format!("-ERR {} \r\n", error).as_bytes().to_vec()
}
