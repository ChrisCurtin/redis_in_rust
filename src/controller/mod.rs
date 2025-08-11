use app_properties::AppProperties;
use crate::thread_pool::ThreadPool;
use crate::tokenizer;
use std::{ io, io::{ prelude::*}, net::{TcpListener, TcpStream}};

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

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        pool.execute(|| {
            handle_connection(stream);
        });
    }

    println!("Shutting down.");
    
}
fn handle_connection(mut stream: TcpStream) {

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
                        stream.write_all(b"+OK\r\n").unwrap();
                    },
                    Err(error) => {
                        println!("Error {:?}", error.get_message());
                        stream.write_all(error.get_message().as_bytes()).unwrap();
                    }
                }
            },
            Err(msg) => {
                println!("{:?}", msg.to_string());
                return
            }
        };

    }
}
