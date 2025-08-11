
mod commands;
mod tokenizer;
mod string_executor;
mod thread_pool;
mod controller;
// TODO - things to look at while builing this code
// 1. Axum for restful APIs



fn main() {
    controller::initialize_controller();

}
