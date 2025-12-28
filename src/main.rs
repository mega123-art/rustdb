use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
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
            File::create(path).expect("Failed to create file");
            println!("Database '{}' created.", name);
        }
    }

    fn use_db(&mut self, name: &str) {
        let path = format!("{}.db", name);
        if !Path::new(&path).exists() {
            println!("Error: Database '{}' not found.", name);
            return;
        }

        
        self.index.clear();
        let file = File::open(&path).unwrap();
        let reader = BufReader::new(file);

        for line in reader.lines() {
            if let Ok(content) = line {
                let parts: Vec<&str> = content.splitn(2, ',').collect();
                if parts.len() == 2 {
                    self.index.insert(parts[0].to_string(), parts[1].to_string());
                }
            }
        }

        self.active_name = Some(name.to_string());
        println!("Switched to database '{}'.", name);
    }

    fn set_fast(&mut self, key: String, value: String) {
    if let Some(ref name) = self.active_name {
        
        self.index.insert(key.clone(), value.clone());

        
        let mut file = OpenOptions::new()
            .append(true)
            .open(format!("{}.db", name))
            .unwrap();
        
        writeln!(file, "{},{}", key, value).unwrap();
        println!("OK (appended)");
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
            for (key, value) in &self.index {
                writeln!(temp_file, "{},{}", key, value).expect("Write error");
            }

            
            
            fs::rename(temp_path, original_path).expect("Failed to replace database file");
            
            println!("Compaction complete. Storage optimized.");
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