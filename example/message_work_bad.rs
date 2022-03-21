use omnipaxos_core::{
    sequence_paxos::{SequencePaxos, SequencePaxosConfig},
    storage::{memory_storage::MemoryStorage},
    messages::Message,
};
use structopt::StructOpt;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use serde::{Serialize, Deserialize};

#[derive(Debug, StructOpt, Serialize, Deserialize)]
struct Node {
    
    #[structopt(long)]
    id: u64,
    
    #[structopt(long)]
    peers: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[tokio::main]
async fn main() {
    
    let node = Node::from_args();

    // configuration with id 1 and the following cluster
    
    // TODO: what does the config do? initializes sp struct. look into moving to config files
    let configuration_id = node.id;
    let _cluster = vec![1, 2, 3];

    // pid and peers set using commandline

    let mut sp_config = SequencePaxosConfig::default();
    sp_config.set_configuration_id(configuration_id.try_into().unwrap());
    sp_config.set_pid(node.id);
    sp_config.set_peers(node.peers.to_vec());

    let storage = MemoryStorage::<KeyValue, ()>::default(); // TODO: look into snapshots later on
    // TODO: sp was mutable before. check if needed later on? 
    let sp = SequencePaxos::with(sp_config, storage);  // local sequence paxos instance, check

    //need explicit sq instance ownership. use channels
    // TODO: receiver was mut before. check if needed later on?
    let (sender1, receiver) = mpsc::channel(32);

    let sender2 = sender1.clone();
    
    tokio::spawn(async move {
        sp_command_manager(sp, receiver).await;
    });

    // start thread for calling sending messages
    tokio::spawn(async move {
        periodically_send_outgoing_msgs(sender1).await;
    });
 

    // start of networking code. TODO: move to seperate function?
    //let node = Node::from_args(); moved to top for config copying node id.
    let mut listen_addr: String = "127.0.0.1:".to_owned();
    let listen_port: u64 = 50000 + node.id;
    listen_addr.push_str(&listen_port.to_string().to_owned()); 
    
    let listener = TcpListener::bind(listen_addr).await.unwrap();
    
    println!("Listening as: Id: {}, Peers: {:?}", node.id, node.peers);
    
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let sender_n = sender2.clone();
    
        tokio::spawn(async move {
            
            handle_reads(socket, node.id, sender_n).await;
        });
    }
}

// reads the message. 
async fn handle_reads(read_socket: TcpStream, id: u64, sender: mpsc::Sender<(&str, Message<KeyValue, ()>)>) {
    
    let (mut reader, _) = io::split(read_socket);
    
    let mut buffer = vec![1; 128];
    loop {
        let n = reader.read(&mut buffer).await.unwrap();
        
        if n == 0 {
            break;
        }
        
        if n == 127 {
            println!("MESSAGES STARTING GET TOO BIG, BUFFER NOT BIG ENOUGH"); //FIXME: any better solutions than fixed buffer?
        }

        let msg_dec: Message<KeyValue, ()> = bincode::deserialize(&buffer[..n]).unwrap();
        
        println!("Id {} got message: {:?}", id, msg_dec);

        // sp.handle(msg_dec);
        sender.send(("handle", msg_dec)).await.unwrap();
    }
    
}

// sends message TODO: look into having one tread manage all sends instead of multiple threads a.la tokio channeling?
async fn periodically_send_outgoing_msgs(sender: mpsc::Sender<(&str, Message<KeyValue, ()>)>) {
    use std::{thread, time};
    use omnipaxos_core::messages::PaxosMsg::PrepareReq;
    
    // periodically check outgoing messages and send all in list
    // FIXME: Cange to 1 ms
    loop {
        thread::sleep(time::Duration::from_millis(10));
        sender.send(("send_outgoing", Message::with(0,0,PrepareReq))).await.unwrap();
    }
}

async fn sp_command_manager(mut sp: SequencePaxos<KeyValue, (), MemoryStorage<KeyValue, ()>>, 
    mut receiver: mpsc::Receiver<(&str, Message<KeyValue, ()>)>) {

    while let Some(action) = receiver.recv().await {
        match (action.0, action.1) {
            ("handle", msg) => {
                sp.handle(msg);
            }
            ("send_outgoing", ..) => {
                for out_msg in sp.get_outgoing_msgs() {
                    // we send message to someone else. receiveer should be pid which we can add to port for our communication protocol
                    let receiver = out_msg.to;
                    
                    let stream = TcpStream::connect(format!("127.0.0.1:{}", 50000 + receiver)).await.unwrap();
                    let (_reader, mut writer) = io::split(stream);
            
                    let msg_enc: Vec<u8> = bincode::serialize(&out_msg).unwrap();
                    writer.write_all(&msg_enc).await.unwrap();
                }
            }
            other => {
                println!("Action not implemented: {:?}", other);
            }
        }
    }
}