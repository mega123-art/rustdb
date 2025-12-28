
use std::fs::{File, OpenOptions};
use std::io::{self, Write, Read, Seek, SeekFrom};
use std::path::Path;

#[derive(Debug)]
struct BTreeNode {
    is_leaf: bool,
    keys: Vec<String>,
    offsets: Vec<u64>, 
    children: Vec<Box<BTreeNode>>,
}

struct DbEngine {
    active_name: Option<String>,
    root: BTreeNode, 
}

impl BTreeNode {
    fn new(is_leaf: bool) -> Self {
        Self {
            is_leaf,
            keys: Vec::new(),
            offsets: Vec::new(),
            children: Vec::new(),
        }
    }

    fn search(node: &BTreeNode, key: &str) -> Option<u64> {
        let mut i = 0;
        while i < node.keys.len() && key > &node.keys[i] {
            i += 1;
        }

        if i < node.keys.len() && key == &node.keys[i] {
            return Some(node.offsets[i]);
        }

        if node.is_leaf {
            return None;
        }

        // Fix: Use Self::search for recursion
        Self::search(&node.children[i], key)
    }

    fn split_child(parent: &mut BTreeNode, index: usize) {
        let t = 3; 
        let child = &mut parent.children[index];
        let mut new_node = BTreeNode::new(child.is_leaf);

        // Split data at the degree 't'
        new_node.keys = child.keys.split_off(t);
        new_node.offsets = child.offsets.split_off(t);

        if !child.is_leaf {
            new_node.children = child.children.split_off(t);
        }

        let median_key = child.keys.pop().unwrap();
        let median_offset = child.offsets.pop().unwrap();

        parent.keys.insert(index, median_key);
        parent.offsets.insert(index, median_offset);
        parent.children.insert(index + 1, Box::new(new_node));
    }
}

impl DbEngine {
    fn new() -> Self {
        Self {
            active_name: None,
            root: BTreeNode::new(true),
        }
    }

    // FIX: To avoid double-borrowing, we make this a standalone method 
    // that takes the node as a separate argument.
    fn insert_non_full(node: &mut BTreeNode, key: String, offset: u64) {
        let mut i = node.keys.len() as i64 - 1;

        if node.is_leaf {
            node.keys.push(String::new());
            node.offsets.push(0);
            while i >= 0 && key < node.keys[i as usize] {
                node.keys[(i + 1) as usize] = node.keys[i as usize].clone();
                node.offsets[(i + 1) as usize] = node.offsets[i as usize];
                i -= 1;
            }
            node.keys[(i + 1) as usize] = key;
            node.offsets[(i + 1) as usize] = offset;
        } else {
            while i >= 0 && key < node.keys[i as usize] {
                i -= 1;
            }
            let idx = (i + 1) as usize;
            if node.children[idx].keys.len() == (2 * 3 - 1) {
                BTreeNode::split_child(node, idx);
                if key > node.keys[idx] {
                    Self::insert_non_full(&mut node.children[idx + 1], key, offset);
                    return;
                }
            }
            Self::insert_non_full(&mut node.children[idx], key, offset);
        }
    }

    fn insert_into_tree(&mut self, key: String, offset: u64) {
        let t = 3;
        if self.root.keys.len() == (2 * t - 1) {
            let new_root_node = BTreeNode::new(false);
            let old_root = std::mem::replace(&mut self.root, new_root_node);
            self.root.children.push(Box::new(old_root));
            BTreeNode::split_child(&mut self.root, 0);
        }
        // Call the static-style helper to avoid borrowing 'self' twice
        Self::insert_non_full(&mut self.root, key, offset);
    }

    // --- Database File Commands ---

    fn create_db(&self, name: &str) {
        let path = format!("{}.db", name);
        if Path::new(&path).exists() {
            println!("Error: Database '{}' already exists.", name);
        } else {
            let mut file = File::create(path).expect("Failed to create file");
            file.write_all(b"RDBX").unwrap();
            file.write_all(&1u32.to_le_bytes()).unwrap();
            println!("Database '{}' created.", name);
        }
    }

    fn use_db(&mut self, name: &str) {
        let path = format!("{}.db", name);
        if !Path::new(&path).exists() { return; }

        self.root = BTreeNode::new(true); // Clear old tree
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(8)).unwrap();

        loop {
            let current_pos = file.stream_position().unwrap(); 
            let mut k_len_buf = [0u8; 4];
            let mut v_len_buf = [0u8; 4];

            if file.read_exact(&mut k_len_buf).is_err() { break; } 
            file.read_exact(&mut v_len_buf).unwrap();

            let k_len = u32::from_le_bytes(k_len_buf) as usize;
            let v_len = u32::from_le_bytes(v_len_buf) as usize;

            let mut k_buf = vec![0u8; k_len];
            file.read_exact(&mut k_buf).unwrap();
            let key = String::from_utf8(k_buf).unwrap();

            file.seek(SeekFrom::Current(v_len as i64)).unwrap();
            self.insert_into_tree(key, current_pos);
        }
        
        self.active_name = Some(name.to_string());
        println!("Switched to '{}'. B-Tree Index built.", name);
    }

    fn set_fast(&mut self, key: String, value: String) {
        if let Some(ref name) = self.active_name {
            let path = format!("{}.db", name);
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();

            let offset = file.metadata().unwrap().len();
            let k_bytes = key.as_bytes();
            let v_bytes = value.as_bytes();

            file.write_all(&(k_bytes.len() as u32).to_le_bytes()).unwrap();
            file.write_all(&(v_bytes.len() as u32).to_le_bytes()).unwrap();
            file.write_all(k_bytes).unwrap();
            file.write_all(v_bytes).unwrap();

            self.insert_into_tree(key, offset);
            println!("OK (at {})", offset);
        }
    }

    fn get(&self, key: &str) {
        let offset = match BTreeNode::search(&self.root, key) {
            Some(pos) => pos,
            None => { println!("(nil)"); return; }
        };

        if let Some(ref name) = self.active_name {
            let mut file = File::open(format!("{}.db", name)).unwrap();
            file.seek(SeekFrom::Start(offset)).unwrap();

            let mut lens = [0u8; 8];
            file.read_exact(&mut lens).unwrap();
            let k_len = u32::from_le_bytes([lens[0], lens[1], lens[2], lens[3]]) as i64;
            let v_len = u32::from_le_bytes([lens[4], lens[5], lens[6], lens[7]]) as usize;

            file.seek(SeekFrom::Current(k_len)).unwrap();
            let mut v_buf = vec![0u8; v_len];
            file.read_exact(&mut v_buf).unwrap();
            println!("\"{}\"", String::from_utf8(v_buf).unwrap());
        }
    }
}

fn main() {
    let mut engine = DbEngine::new();
    println!("RustDB 0.1.0 - B-Tree Mode");

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
            "CREATE" => if parts.len() > 1 { engine.create_db(parts[1]) },
            "USE" => if parts.len() > 1 { engine.use_db(parts[1]) },
            "SET" => if parts.len() > 2 { engine.set_fast(parts[1].to_string(), parts[2..].join(" ")) },
            "GET" => if parts.len() > 1 { engine.get(parts[1]) },
            "EXIT" => break,
            _ => println!("Unknown command."),
        }
    }
}