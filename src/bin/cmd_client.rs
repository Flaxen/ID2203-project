use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
// use serde::{Serialize, Deserialize};

#[tokio::main]
async fn main() {
    
    let temptemp = vec![116, 104, 101, 32, 107, 101, 121, 32, 112, 114, 111, 118, 105, 100, 101, 100, 32, 98, 121, 32, 99, 109, 100, 32, 105, 115, 32, 62, 91, 49, 48, 55, 44, 32, 49, 48, 49, 44, 32, 49, 50, 49, 93, 60, 107, 101, 121, 58, 32, 62, 75, 101, 121, 86, 97, 108, 117, 101, 32, 123, 32, 107, 101, 121, 58, 32, 34, 107, 101, 121, 34, 44, 32, 118, 97, 108, 117, 101, 58, 32, 50, 32, 125, 60, 13, 10, 32, 32, 32, 32, 32, 101, 110, 100, 32, 32, 32, 32];
    let mut tempString = String::from_utf8(temptemp).unwrap();
    println!("First print is:{}", tempString);

    let temptemp2 = vec![116, 104, 101, 32, 107, 101, 121, 32, 112, 114, 111, 118, 105, 100, 101, 100, 32, 98, 121, 32, 99, 109, 100, 32, 105, 115, 32, 62, 91, 49, 48, 55, 44, 32, 49, 48, 49, 44, 32, 49, 50, 49, 93, 60, 107, 101, 121, 58, 32, 62, 75, 101, 121, 86, 97, 108, 117, 101, 32, 123, 32, 107, 101, 121, 58, 32, 34, 107, 101, 121, 34, 44, 32, 118, 97, 108, 117, 101, 58, 32, 50, 32, 125, 60, 13, 10, 107, 101, 121, 58, 32, 62, 75, 101, 121, 86, 97, 108, 117, 101, 32, 123, 32, 107, 101, 121, 58, 32, 34, 107, 101, 121, 34, 44, 32, 118, 97, 108, 117, 101];
    tempString = String::from_utf8(temptemp2).unwrap();
    println!("Second print is:{}", tempString);

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
        // println!("String to send now is:{:?}", line.trim().as_bytes());
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
            
            println!("enc:{:?}", &buffer[..n]);
            let msg_dec: String = bincode::deserialize(&buffer[..n]).unwrap();
            println!("Output: {:?}", msg_dec);
        }
    }
}