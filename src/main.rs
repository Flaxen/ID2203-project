use omnipaxos_core::{
    sequence_paxos::{SequencePaxos, SequencePaxosConfig},
    storage::{memory_storage::MemoryStorage},
    ballot_leader_election::{BallotLeaderElection, BLEConfig, messages::BLEMessage},
    messages::Message,
};

use structopt::StructOpt;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use serde::{Serialize, Deserialize};
use std::{thread, time};    


// node struct. mostly used for getting id and peers from command line
#[derive(Debug, StructOpt, Serialize, Deserialize)]
struct Node {
    
    #[structopt(long)]
    id: u64,
    
    #[structopt(long)]
    peers: Vec<u64>,
}

// kv struct for the store
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[tokio::main]
async fn main() {
    let node = Node::from_args();    
    
    // pid and peers set using commandline
    let configuration_id = node.id;
    // let _cluster = vec![1, 2, 3]; // unsure what this did to begin with tbh

    let mut sp_config = SequencePaxosConfig::default();
    sp_config.set_configuration_id(configuration_id.try_into().unwrap());
    sp_config.set_pid(node.id);
    sp_config.set_peers(node.peers.to_vec());
    
    let storage = MemoryStorage::<KeyValue, ()>::default(); // TODO: look into snapshots if there is time
    let sp = SequencePaxos::with(sp_config, storage);

    let mut ble_config = BLEConfig::default();
    ble_config.set_pid(node.id);
    ble_config.set_peers(node.peers.to_vec());
    ble_config.set_hb_delay(40);
    
    let ble = BallotLeaderElection::with(ble_config);
    
    //need explicit sq instance ownership. use channels
    let (sender1, receiver) = mpsc::channel(32);
    let sender2 = sender1.clone();
    let sender3 = sender1.clone();
    let sender4 = sender1.clone();
    let sender5 = sender1.clone();
    
    // thread for managing local sp and ble instances
    tokio::spawn(async move {
        sp_command_manager(sp, receiver, ble).await;
    });

    // thread for calling the sending of outgoing sp & ble messages
    tokio::spawn(async move {
        periodically_send_outgoing_msgs(sender1).await;
    });

    // thread for reading command line for user input
    tokio::spawn(async move {
        command_line_listener(sender3, &node.id).await;
    });

    // thread for ble time checks
    tokio::spawn(async move {
        ble_timer(sender4).await;
    });

    // thread for ble network communication
    tokio::spawn(async move {
        ble_net(sender5, &node.id).await;
    });
 
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
async fn handle_reads(read_socket: TcpStream, _id: u64, sender: mpsc::Sender<(&str, Vec<u8>)>) {
    
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

        sender.send(("handle", (&buffer[..n]).to_vec())).await.unwrap();
    }
}

async fn periodically_send_outgoing_msgs(sender: mpsc::Sender<(&str, Vec<u8>)>) {
    // periodically check outgoing messages and send all in list
    loop {
        thread::sleep(time::Duration::from_millis(1));
        sender.send(("send_outgoing", vec![])).await.unwrap();
    }
}

async fn ble_timer(sender: mpsc::Sender<(&str, Vec<u8>)>) {
    // every 10 ms run handle leader
    loop {
        thread::sleep(time::Duration::from_millis(40));
        sender.send(("leader", vec![])).await.unwrap();
    }
}

async fn sp_command_manager(mut sp: SequencePaxos<KeyValue, (), MemoryStorage<KeyValue, ()>>, 
                            mut receiver: mpsc::Receiver<(&str, Vec<u8>)>, mut ble: BallotLeaderElection) {

    while let Some(action) = receiver.recv().await {
        match (action.0, action.1) {
            ("handle", msg_enc) => {
                let msg: Message<KeyValue, ()> = bincode::deserialize(&msg_enc).unwrap();
                sp.handle(msg);
            }
            ("send_outgoing", ..) => {

                // send sequence paxos messages
                for out_msg in sp.get_outgoing_msgs() {
                    // we send message to someone else. receiveer should be pid which we can add to port for our communication protocol
                    let receiver = out_msg.to;
                    match TcpStream::connect(format!("127.0.0.1:{}", 50000 + receiver)).await {
                        Ok(stream) => {
                            let (_reader, mut writer) = io::split(stream);
                    
                            let msg_enc: Vec<u8> = bincode::serialize(&out_msg).unwrap();
                            writer.write_all(&msg_enc).await.unwrap();
                        },
                        Err(_) => println!("Could not open sp message connection to some or more nodes. Will try to send next call"),
                    }
                }

                for out_msg in ble.get_outgoing_msgs() {
                    let receiver = out_msg.to;
                    match TcpStream::connect(format!("127.0.0.1:{}", 60000 + receiver)).await {
                        Ok(stream) => {
                        // send ble messages
                            let (_reader, mut writer) = io::split(stream);
                            
                            let msg_enc: Vec<u8> = bincode::serialize(&out_msg).unwrap();
                            writer.write_all(&msg_enc).await.unwrap();
                        },
                        Err(_) => println!("Could not open ble message connection. Will try to send next call"),
                    }
                }
            }
            ("read", key_enc) => {
                // get key for reading
                let key: String = bincode::deserialize(&key_enc).unwrap();

                // get all decided values from index
                let decided: Option<Vec<LogEntry<KeyValue, ()>>> = sp.read_decided_suffix(0);

                match decided {
                    Some(vec) => {
                        write_res_to_cmd(vec.to_vec(), key).await;
                    },
                    _ => println!("EROEROEROEROEROER"),
                }
            },
            ("write", kv_enc) => {
                let kv: KeyValue = bincode::deserialize(&kv_enc).unwrap();
                sp.append(kv).expect("Failed to append");
                println!("append has been called");

            },
            ("leader", ..) => {
                if let Some(leader) = ble.tick() {
                    sp.handle_leader(leader);
                }
            }
            ("ble_handle", msg_enc) => {
                let msg_dec: BLEMessage = bincode::deserialize(&msg_enc).unwrap();
                ble.handle(msg_dec);
            },
            other => {
                println!("Action not implemented: {:?}", other);
            }
        }
    }
}

// listens for read and write commands from terminal
async fn command_line_listener(sender: mpsc::Sender<(&str, Vec<u8>)>, id: &u64) {
    // connection for terminal on port [65k, ..)
    let mut listen_addr: String = "127.0.0.1:".to_owned();
    let listen_port: u64 = 65000 + id;
    listen_addr.push_str(&listen_port.to_string().to_owned()); 
    
    let listener = TcpListener::bind(listen_addr).await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let (mut reader, _) = io::split(socket);
        let mut buffer = vec![1; 128];
        loop {
            let n = reader.read(&mut buffer).await.unwrap();
            
            if n == 0 {
                break;
            }
            if n == 127 {
                println!("MESSAGES STARTING GET TOO BIG, BUFFER NOT BIG ENOUGH"); //FIXME: any better solutions than fixed buffer?
            }
            let msg_dec: String = bincode::deserialize(&buffer[..n]).unwrap();
            let v:Vec<&str> = msg_dec.split(" ").collect();

            match v[0] {
                "read" => {
                    // send string of key to read
                    sender.send(("read", bincode::serialize(&String::from(v[1])).unwrap())).await.unwrap();
                },
                "write" => {
                    let kv = KeyValue{key: String::from(v[1].to_string()), value: v[2].trim().parse().expect("Value needs to be a number!")};

                    sender.send(("write", bincode::serialize(&kv).unwrap())).await.unwrap();
                },
                cmd => println!("Not viable command: {:?}", cmd),
    
            }
        }
    }
}

async fn ble_net(sender: mpsc::Sender<(&str, Vec<u8>)>, id: &u64) {
    let mut listen_addr: String = "127.0.0.1:".to_owned();
    let listen_port: u64 = 60000 + id;
    listen_addr.push_str(&listen_port.to_string().to_owned()); 
    
    let listener = TcpListener::bind(listen_addr).await.unwrap();
    
    println!("Listening for ble");
    
    loop {
        let (socket, _) = listener.accept().await.unwrap();    
        let (mut reader, _) = io::split(socket);
        let mut buffer = vec![1; 128];
        loop {
            let n = reader.read(&mut buffer).await.unwrap();
            
            if n == 0 {
                break;
            }
            
            if n == 127 {
                println!("MESSAGES STARTING GET TOO BIG, BUFFER NOT BIG ENOUGH"); //FIXME: any better solutions than fixed buffer?
            }
    
            sender.send(("ble_handle", (&buffer[..n]).to_vec())).await.unwrap();
        }
    }
}

use omnipaxos_core::util::LogEntry::{self, Decided};
async fn write_res_to_cmd(vec: Vec<LogEntry<'_, KeyValue, ()>>, key: String) {
    // prepare stream for reply to command window
    let stream = TcpStream::connect(format!("127.0.0.1:{}", 65000)).await.unwrap();
    let (_reader, mut writer) = tokio::io::split(stream);  
    
    // loop backwards through decided
    let mut index = vec.len()-1;
    let mut response = String::new();
    let mut found_key = false;
    loop {

        match vec[index] {
            Decided(kv) => {
                if kv.key == key {
                    response.push_str(format!("{}", kv.value).as_str());
                    found_key = true;
                }
            },
            _ => println!("nothing now, checking next"),
        };

        if index == 0 || found_key {
            break;
        }
        index-=1;
    }

    // write found message, if nothing found error is thrown in command_manager. nothing shown here
    let msg_enc: Vec<u8> = bincode::serialize(&response).unwrap();
    writer.write_all(&msg_enc).await.unwrap();
}
