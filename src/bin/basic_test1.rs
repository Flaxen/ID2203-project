use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use std::{thread, time};    


#[tokio::main]
async fn main() {
    
    // listens to socket for output to print
    tokio::spawn(async move {
        io_printer().await;
    });
    
    // loop to take commands and send to socket
    println!("Ready to read commands");
    
    let commands: Vec<&str> = vec![
        "1 write key 1",
        "1 read key",
        "2 read key",
        "3 read key",
        
        "2 write key2 2",
        "1 read key2",
        "2 read key2",
        "3 read key2",

        "3 write key3 3",
        "1 read key3",
        "2 read key3",
        "3 read key3",

    ];    

    thread::sleep(time::Duration::from_millis(5000));
    

    for line in commands {
        let mut line = line.to_string();
        // get port to direct message to correct node
        let v:Vec<&str> = line.split(" ").collect();
        let id: u64 = v[0].trim().parse().expect("Id needs to be a number!");

        let stream = TcpStream::connect(format!("127.0.0.1:{}", 65000 + id)).await.unwrap();
        let (_reader, mut writer) = tokio::io::split(stream);    

        line.replace_range(..2, "");
        let msg_enc: Vec<u8> = bincode::serialize(&line.trim()).unwrap();

        println!("Sending command:{}, to id {}", &line.trim(), id);

        writer.write_all(&msg_enc).await.unwrap();
        thread::sleep(time::Duration::from_millis(1000));


    }

    loop {

    }
}

async fn io_printer() {
    let mut listen_addr: String = "127.0.0.1:".to_owned();
    let listen_port: u64 = 65000;
    listen_addr.push_str(&listen_port.to_string().to_owned()); 
    
    let listener = TcpListener::bind(listen_addr).await.unwrap();
    
    println!("Listening for output");
    
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
            println!("Output: {:?}", msg_dec);
        }
    }
}