# ID2203-project
KV-store omnipaxos


## server
example of how to start three nodes in the same cluster

`cargo run --bin kv-store -- --id 1 --peers 2 3`  
`cargo run --bin kv-store -- --id 2 --peers 3 2`  
`cargo run --bin kv-store -- --id 3 --peers 1 2`  

It is highly recommended that you edit src/main.rs before running as this will give you a build lock on the other nodes. This gives you some time to start all nodes, which will start the nodes at roughly the same time. This will make sure that the nodes are in synch as causes less issues. Random comments do the trick nicely.


## client
To start the client command line you can use the following command

`cargo run --bin cmd_client`

commands in the client follow the following structure

NODE_ID READ/WRITE KEY POSSIBLE_VALUE

possible_value is only to be provided in cases where you write
client command examples:

`2 write key 2`  
`1 read key`  
`3 read key`  
`1 write key2 42`  
