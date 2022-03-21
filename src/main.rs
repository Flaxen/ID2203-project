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

    let mut ble_config = BLEConfig::default();
    ble_config.set_pid(node.id);
    ble_config.set_peers(node.peers.to_vec());
    ble_config.set_hb_delay(40);     // a leader timeout of 20 ticks
    
    let ble = BallotLeaderElection::with(ble_config);
    
    //need explicit sq instance ownership. use channels
    // TODO: receiver was mut before. check if needed later on?
    let (sender1, receiver) = mpsc::channel(32);
    let sender2 = sender1.clone();
    let sender3 = sender1.clone();
    let sender4 = sender1.clone();
    let sender5 = sender1.clone();
    
    // thread for managing local sp instance
    tokio::spawn(async move {
        sp_command_manager(sp, receiver, ble).await;
    });

    // thread for calling sending messages
    tokio::spawn(async move {
        periodically_send_outgoing_msgs(sender1).await;
    });

    // thread for reading command line for user input
    tokio::spawn(async move {
        command_line_listener(sender3, &node.id).await;
    });

    // thread for BLE time checks
    tokio::spawn(async move {
        ble_timer(sender4).await;
    });

    tokio::spawn(async move {
        ble_net(sender5, &node.id).await;
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

        //let msg_dec: Message<KeyValue, ()> = bincode::deserialize(&buffer[..n]).unwrap();
        //TODO: no need to deserialize if we send buffer
        //println!("Id {} got message: {:?}", id, msg_dec);

        // sp.handle(msg_dec);
        sender.send(("handle", (&buffer[..n]).to_vec())).await.unwrap();
    }
    
}

// sends message TODO: look into having one tread manage all sends instead of multiple threads a.la tokio channeling?
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
                // send seqpax messages
                for out_msg in sp.get_outgoing_msgs() {
                    // we send message to someone else. receiveer should be pid which we can add to port for our communication protocol
                    let receiver = out_msg.to;
                    
                    let stream = TcpStream::connect(format!("127.0.0.1:{}", 50000 + receiver)).await.unwrap();
                    let (_reader, mut writer) = io::split(stream);
            
                    let msg_enc: Vec<u8> = bincode::serialize(&out_msg).unwrap();
                    writer.write_all(&msg_enc).await.unwrap();
                    //println!("actually sent a message");
                }
                // send ble messages
                for out_msg in ble.get_outgoing_msgs() {
                    let receiver = out_msg.to;

                    let stream = TcpStream::connect(format!("127.0.0.1:{}", 60000 + receiver)).await.unwrap();
                    let (_reader, mut writer) = io::split(stream);
            
                    let msg_enc: Vec<u8> = bincode::serialize(&out_msg).unwrap();
                    writer.write_all(&msg_enc).await.unwrap();
                    //println!("actually sent a message");
                }
            }
            ("read", key_enc) => {
                // get key for reading
                let key: String = bincode::deserialize(&key_enc).unwrap();

                // get all decided values from index
                let decided: Option<Vec<LogEntry<KeyValue, ()>>> = sp.read_decided_suffix(0);

                match decided {
                    Some(vec) => {
                        // tokio::spawn(async move  {
                            write_res_to_cmd(vec.to_vec(), key).await;
                        // });
                    },
                    _ => println!("EROEROEROEROEROER"),
                }

                // find_and_print_kv(key, sp.read_entries(..sp.storage.get_log_len()));

            },
            ("write", kv_enc) => {
                let kv: KeyValue = bincode::deserialize(&kv_enc).unwrap();
                sp.append(kv).expect("Failed to append");
                println!("append has been called");

            },
            ("leader", ..) => {
                if let Some(leader) = ble.tick() {
                    // println!("DOING BLE TICK");
                    // a new leader is elected, pass it to SequencePaxos.
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

async fn command_line_listener(sender: mpsc::Sender<(&str, Vec<u8>)>, id: &u64) {
    // use std::io;
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
                    // make and send keyvalue
                    let kkkey: String = v[1].to_string();
                    // println!("HEAR YE HEAR YE, KEY IS:{:?}:", kkkey.as_bytes());
                    let kv = KeyValue{key: String::from(v[1]), value: v[2].trim().parse().expect("Value needs to be a number!")};
                    // println!("HEAR YE HEAR YE, KV.KEY IS:{:?}:", kv.key.as_bytes());

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
        // let sender_n = sender.clone();
    
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
    
            // let msg_dec: BLEMessage = bincode::deserialize(&buffer[..n]).unwrap();
            //TODO: no need to deserialize if we send buffer
            //println!("got BLE message: {:?}", msg_dec);
    
            // sp.handle(msg_dec);
            sender.send(("ble_handle", (&buffer[..n]).to_vec())).await.unwrap();
        }
    }
}

use omnipaxos_core::util::LogEntry::{self, Decided};
async fn write_res_to_cmd(vec: Vec<LogEntry<'_, KeyValue, ()>>, key: String) {
    // prepare stream for reply to command window
    let stream = TcpStream::connect(format!("127.0.0.1:{}", 65000)).await.unwrap();
    let (_reader, mut writer) = tokio::io::split(stream);    
    let mut index = vec.len()-1;
    // let response = format!("got: {:?}", vec);
    let mut response = String::new();
    response.push_str(format!("the key provided by cmd is >{:?}<", key.as_bytes()).as_str());
    let mut end = false;
    loop {

        match vec[index] {
            Decided(kv) => {
                if kv.key == key {
                    response.push_str(format!("key: >{:?}<\r\n", kv).as_str());
                    // break;
                    end = true;
                }
            },

            _ => println!("nothing now, trying more"),
            // response.push_str(format!("got some error here lollers").as_str())
        };
        
        // response.push_str(format!("some elem: {:?}\n", vec[index]).as_str());
        if index == 0 || end {
            response.push_str(format!("     end    ").as_str());
            break;
        }
        index-=1;
    }
    let msg_enc: Vec<u8> = bincode::serialize(&response).unwrap();
    writer.write_all(&msg_enc).await.unwrap();
    println!("GOT HERE MADAFAKA");
}
