use std::io::{self, Write};
use tonic::Request;
use aegisdb::server::grpc::tinykvpb::tiny_kv_client::TinyKvClient;
use aegisdb::server::grpc::kvrpcpb::{RawGetRequest, RawPutRequest, RawDeleteRequest, RawScanRequest, Context};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 解析命令行参数
    let args: Vec<String> = std::env::args().collect();
    let server_addr = if args.len() > 1 {
        format!("http://{}", args[1])
    } else {
        "http://127.0.0.1:20160".to_string()
    };
    
    println!("=== AegisDB CLI ===");
    println!("Connecting to {}...", server_addr);
    
    // 连接服务器
    let mut client = match TinyKvClient::connect(server_addr.clone()).await {
        Ok(c) => {
            println!("Connected successfully!");
            c
        }
        Err(e) => {
            eprintln!("Failed to connect to server: {}", e);
            eprintln!("Make sure the server is running: aegisdb_server");
            return Err(e.into());
        }
    };
    
    println!("\nAvailable commands:");
    println!("  GET <key> [cf]          - Get value for key (default CF: 'default')");
    println!("  SET <key> <value> [cf] - Set key-value pair (default CF: 'default')");
    println!("  DEL <key> [cf]         - Delete key (default CF: 'default')");
    println!("  SCAN <start_key> <limit> [cf] - Scan keys (default CF: 'default')");
    println!("  QUIT/EXIT              - Exit CLI");
    println!();
    
    // REPL 循环
    loop {
        print!("aegisdb> ");
        io::stdout().flush()?;
        
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        
        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        
        match parts[0].to_uppercase().as_str() {
            "GET" => {
                if parts.len() < 2 {
                    println!("Usage: GET <key> [cf]");
                    continue;
                }
                
                let key = parts[1].as_bytes().to_vec();
                let cf = parts.get(2).map(|s| s.to_string()).unwrap_or_else(|| "default".to_string());
                
                let req = Request::new(RawGetRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    key,
                    cf,
                });
                
                match client.raw_get(req).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.not_found {
                            println!("(nil)");
                        } else if !resp.error.is_empty() {
                            println!("Error: {}", resp.error);
                        } else {
                            let value = String::from_utf8_lossy(&resp.value);
                            println!("\"{}\"", value);
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "SET" => {
                if parts.len() < 3 {
                    println!("Usage: SET <key> <value> [cf]");
                    continue;
                }
                
                let key = parts[1].as_bytes().to_vec();
                let value = parts[2].as_bytes().to_vec();
                let cf = parts.get(3).map(|s| s.to_string()).unwrap_or_else(|| "default".to_string());
                
                let req = Request::new(RawPutRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    key,
                    value,
                    cf,
                });
                
                match client.raw_put(req).await {
                    Ok(_) => println!("OK"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "DEL" | "DELETE" => {
                if parts.len() < 2 {
                    println!("Usage: DEL <key> [cf]");
                    continue;
                }
                
                let key = parts[1].as_bytes().to_vec();
                let cf = parts.get(2).map(|s| s.to_string()).unwrap_or_else(|| "default".to_string());
                
                let req = Request::new(RawDeleteRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    key,
                    cf,
                });
                
                match client.raw_delete(req).await {
                    Ok(_) => println!("OK"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "SCAN" => {
                if parts.len() < 3 {
                    println!("Usage: SCAN <start_key> <limit> [cf]");
                    continue;
                }
                
                let start_key = parts[1].as_bytes().to_vec();
                let limit: u32 = parts[2].parse().unwrap_or(10);
                let cf = parts.get(3).map(|s| s.to_string()).unwrap_or_else(|| "default".to_string());
                
                let req = Request::new(RawScanRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    start_key,
                    limit,
                    cf,
                });
                
                match client.raw_scan(req).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.kvs.is_empty() {
                            println!("(empty list or set)");
                        } else {
                            for (i, kv) in resp.kvs.iter().enumerate() {
                                let key = String::from_utf8_lossy(&kv.key);
                                let value = String::from_utf8_lossy(&kv.value);
                                println!("{}) \"{}\" -> \"{}\"", i + 1, key, value);
                            }
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "QUIT" | "EXIT" | "Q" => {
                println!("Bye!");
                break;
            }
            
            "HELP" | "?" => {
                println!("Available commands:");
                println!("  GET <key> [cf]          - Get value for key");
                println!("  SET <key> <value> [cf] - Set key-value pair");
                println!("  DEL <key> [cf]         - Delete key");
                println!("  SCAN <start_key> <limit> [cf] - Scan keys");
                println!("  QUIT/EXIT              - Exit CLI");
            }
            
            _ => {
                println!("Unknown command: {}. Type HELP for available commands.", parts[0]);
            }
        }
    }
    
    Ok(())
}