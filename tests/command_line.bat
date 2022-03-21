start cargo run --bin kv-store -- --id 1 --peers 2 3
start cargo run --bin kv-store -- --id 2 --peers 3 1
start cargo run --bin kv-store -- --id 3 --peers 1 2

start cargo run --bin cmd_client