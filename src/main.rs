
mod commands;
mod tokenizer;
mod string_executor;
mod thread_pool;
mod controller;
mod index;
mod list_executor;

fn main() {
    // ./redli -h localhost -p 6379 --debug
    env_logger::init();
    controller::initialize_controller();
}
