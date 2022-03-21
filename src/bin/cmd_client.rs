use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    
    // listens to socket for output to print
    tokio::spawn(async move {
        io_printer().await;
    });
    
    // loop to take commands and send to socket
    println!("Ready to read commands");
    
    use std::io;
    loop {
        let mut line = String::new();
        io::stdin()
            .read_line(&mut line)
            .expect("Failed to read line");

        // get port to direct message to correct node
        let v:Vec<&str> = line.split(" ").collect();
        let id: u64 = v[0].trim().parse().expect("Id needs to be a number!");

        let stream = TcpStream::connect(format!("127.0.0.1:{}", 65000 + id)).await.unwrap();
        let (_reader, mut writer) = tokio::io::split(stream);    

        line.replace_range(..2, "");
        let msg_enc: Vec<u8> = bincode::serialize(&line.trim()).unwrap();
        writer.write_all(&msg_enc).await.unwrap();
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