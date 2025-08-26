
mod commands;
mod tokenizer;
mod string_executor;
mod thread_pool;
mod controller;
mod index;

fn main() {
    env_logger::init();
    controller::initialize_controller();
}
