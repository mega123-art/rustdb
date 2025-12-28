use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write, Read};
use std::path::Path;
struct DbEngine {
    active_name: Option<String>,
    index: HashMap<String, String>, 
}

impl DbEngine {
    fn new() -> Self {
        Self {
            active_name: None,
            index: HashMap::new(),
        }
    }

   fn create_db(&self, name: &str) {
    let path = format!("{}.db", name);
    if Path::new(&path).exists() {
        println!("Error: Database '{}' already exists.", name);
    } else {
        let mut file = File::create(path).expect("Failed to create file");
        
        
        file.write_all(b"RDBX").expect("Header write failed");
        
        
        let version: u32 = 1;
        file.write_all(&version.to_le_bytes()).expect("Version write failed");
        
        println!("Database '{}' created with RDBX v1 header.", name);
    }
}

    fn use_db(&mut self, name: &str) {
    let path = format!("{}.db", name);
    if !Path::new(&path).exists() { return; }

    self.index.clear();
    let mut file = File::open(&path).unwrap();
    let mut magic = [0u8; 4];
    let mut version_buf = [0u8; 4];
    file.read_exact(&mut magic).unwrap();
    file.read_exact(&mut version_buf).unwrap();

    if &magic != b"RDBX" {
        println!("Error: Not a valid RustDB file!");
        return;
    }
    
    let version = u32::from_le_bytes(version_buf);
    println!("Loading RDBX version {}...", version);
    
    
    loop {
        let mut k_len_buf = [0u8; 4];
        let mut v_len_buf = [0u8; 4];

        
        if file.read_exact(&mut k_len_buf).is_err() { break; } 
        file.read_exact(&mut v_len_buf).unwrap();

        let k_len = u32::from_le_bytes(k_len_buf) as usize;
        let v_len = u32::from_le_bytes(v_len_buf) as usize;

        
        let mut k_buf = vec![0u8; k_len];
        let mut v_buf = vec![0u8; v_len];
        file.read_exact(&mut k_buf).unwrap();
        file.read_exact(&mut v_buf).unwrap();

        let key = String::from_utf8(k_buf).unwrap();
        let value = String::from_utf8(v_buf).unwrap();
        
        self.index.insert(key, value);
    }
    
    self.active_name = Some(name.to_string());
    println!("Switched to database '{}' (Binary Mode).", name);
}

    fn set_fast(&mut self, key: String, value: String) {
    if let Some(ref name) = self.active_name {
        self.index.insert(key.clone(), value.clone());

        let mut file = OpenOptions::new()
            .append(true)
            .open(format!("{}.db", name))
            .unwrap();

        let k_len = key.len() as u32;
        let v_len = value.len() as u32;

        file.write_all(&k_len.to_le_bytes()).unwrap(); 
        file.write_all(&v_len.to_le_bytes()).unwrap(); 
        file.write_all(key.as_bytes()).unwrap();       
        file.write_all(value.as_bytes()).unwrap();     
        
        println!("OK (binary append)");
    }
}

    fn get(&self, key: &str) {
        if self.active_name.is_none() {
            println!("Error: No database selected.");
            return;
        }
        match self.index.get(key) {
            Some(val) => println!("\"{}\"", val),
            None => println!("(nil)"),
        }
    }

    fn compact(&self) {
    if let Some(ref name) = self.active_name {
        let original_path = format!("{}.db", name);
        let temp_path = format!("{}.temp", name);

        let mut temp_file = File::create(&temp_path).expect("Failed to create temp file");
        
        
        temp_file.write_all(b"RDBX").unwrap();
        let version: u32 = 1;
        temp_file.write_all(&version.to_le_bytes()).unwrap();

        
        for (key, value) in &self.index {
            let k_bytes = key.as_bytes();
            let v_bytes = value.as_bytes();
            let k_len = k_bytes.len() as u32;
            let v_len = v_bytes.len() as u32;

            temp_file.write_all(&k_len.to_le_bytes()).unwrap();
            temp_file.write_all(&v_len.to_le_bytes()).unwrap();
            temp_file.write_all(k_bytes).unwrap();
            temp_file.write_all(v_bytes).unwrap();
        }

        fs::rename(temp_path, original_path).expect("Failed to replace database file");
        println!("Compaction complete (Binary mode). Storage optimized.");
    } else {
        println!("Error: No database selected.");
    }
}
}

fn main() {
    let mut engine = DbEngine::new();
    println!("RustDB 0.1.0 - Total Wild Mode");

    loop {
        let prompt = match &engine.active_name {
            Some(name) => format!("db [{}] > ", name),
            None => "db [none] > ".to_string(),
        };

        print!("{}", prompt);
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() { continue; }

        match parts[0].to_uppercase().as_str() {
            "COMPACT" => engine.compact(),
            "CREATE" => if parts.len() > 1 { engine.create_db(parts[1]) },
            "USE" => if parts.len() > 1 { engine.use_db(parts[1]) },
            "SET" => if parts.len() > 2 { engine.set_fast(parts[1].to_string(), parts[2..].join(" ")) },
            "GET" => if parts.len() > 1 { engine.get(parts[1]) },
            "EXIT" => break,
            _ => println!("Unknown command."),
        }
    }
}