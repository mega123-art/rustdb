use std::fs::{File, OpenOptions};
use std::io::{self, Write, Read, Seek, SeekFrom};
use std::path::Path;

const PAGE_SIZE: usize = 4096;
const T: usize = 3; 

#[derive(Debug, Clone)]
struct BTreeNode {
    offset: u64, 
    is_leaf: bool,
    keys: Vec<String>,
    data_offsets: Vec<u64>,
    child_page_offsets: Vec<u64>,
}

struct DbEngine {
    active_name: Option<String>,
    root_offset: u64,
}

impl BTreeNode {
    fn new(offset: u64, is_leaf: bool) -> Self {
        Self {
            offset,
            is_leaf,
            keys: Vec::new(),
            data_offsets: Vec::new(),
            child_page_offsets: Vec::new(),
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        buf[0] = if self.is_leaf { 1 } else { 0 };
        buf[1..3].copy_from_slice(&(self.keys.len() as u16).to_le_bytes());

        let mut cursor = 3;
        for i in 0..self.keys.len() {
            let k_bytes = self.keys[i].as_bytes();
            buf[cursor..cursor + 4].copy_from_slice(&(k_bytes.len() as u32).to_le_bytes());
            buf[cursor + 4..cursor + 12].copy_from_slice(&self.data_offsets[i].to_le_bytes());
            cursor += 12;
            buf[cursor..cursor + k_bytes.len()].copy_from_slice(k_bytes);
            cursor += k_bytes.len();
        }

        if !self.is_leaf {
            let mut child_cursor = PAGE_SIZE - (self.child_page_offsets.len() * 8);
            for offset in &self.child_page_offsets {
                buf[child_cursor..child_cursor + 8].copy_from_slice(&offset.to_le_bytes());
                child_cursor += 8;
            }
        }
        buf
    }

    fn deserialize(offset: u64, buf: &[u8]) -> Self {
        let is_leaf = buf[0] == 1;
        let num_keys = u16::from_le_bytes([buf[1], buf[2]]) as usize;
        let mut keys = Vec::new();
        let mut data_offsets = Vec::new();

        let mut cursor = 3;
        for _ in 0..num_keys {
            let k_len = u32::from_le_bytes(buf[cursor..cursor+4].try_into().unwrap()) as usize;
            let d_offset = u64::from_le_bytes(buf[cursor+4..cursor+12].try_into().unwrap());
            cursor += 12;
            let key = String::from_utf8(buf[cursor..cursor+k_len].to_vec()).unwrap();
            cursor += k_len;
            keys.push(key);
            data_offsets.push(d_offset);
        }

        let mut child_page_offsets = Vec::new();
        if !is_leaf {
            let mut child_cursor = PAGE_SIZE - ((num_keys + 1) * 8);
            for _ in 0..=num_keys {
                child_page_offsets.push(u64::from_le_bytes(buf[child_cursor..child_cursor+8].try_into().unwrap()));
                child_cursor += 8;
            }
        }
        Self { offset, is_leaf, keys, data_offsets, child_page_offsets }
    }
}

impl DbEngine {
    fn new() -> Self {
        Self { active_name: None, root_offset: 0 }
    }

    fn active_path(&self) -> String {
        format!("{}.db", self.active_name.as_ref().expect("No DB selected"))
    }

    fn load_node(&self, offset: u64) -> BTreeNode {
        let mut file = File::open(self.active_path()).unwrap();
        let mut buf = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut buf).unwrap();
        BTreeNode::deserialize(offset, &buf)
    }

    fn save_node(&self, node: &BTreeNode) {
        let mut file = OpenOptions::new().write(true).open(self.active_path()).unwrap();
        file.seek(SeekFrom::Start(node.offset)).unwrap();
        file.write_all(&node.serialize()).unwrap();
    }

    fn update_root_ptr(&mut self, new_offset: u64) {
        let mut file = OpenOptions::new().write(true).open(self.active_path()).unwrap();
        file.seek(SeekFrom::Start(8)).unwrap();
        file.write_all(&new_offset.to_le_bytes()).unwrap();
        self.root_offset = new_offset;
    }

    fn allocate_page(&self) -> u64 {
        File::open(self.active_path()).unwrap().metadata().unwrap().len()
    }

    
    fn split_child(&self, parent: &mut BTreeNode, i: usize) {
        let mut child = self.load_node(parent.child_page_offsets[i]);
        let new_node_offset = self.allocate_page();
        let mut new_node = BTreeNode::new(new_node_offset, child.is_leaf);

        
        new_node.keys = child.keys.split_off(T);
        new_node.data_offsets = child.data_offsets.split_off(T);
        if !child.is_leaf {
            new_node.child_page_offsets = child.child_page_offsets.split_off(T);
        }

        let mid_key = child.keys.pop().unwrap();
        let mid_data = child.data_offsets.pop().unwrap();

        parent.keys.insert(i, mid_key);
        parent.data_offsets.insert(i, mid_data);
        parent.child_page_offsets.insert(i + 1, new_node_offset);

        
        self.save_node(&child);
        self.save_node(&new_node);
        self.save_node(parent);
    }

    fn insert_non_full(&self, node_offset: u64, key: String, data_offset: u64) {
        let mut node = self.load_node(node_offset);
        let mut i = (node.keys.len() as i64) - 1;

        if node.is_leaf {
            node.keys.push(String::new());
            node.data_offsets.push(0);
            while i >= 0 && key < node.keys[i as usize] {
                node.keys[(i + 1) as usize] = node.keys[i as usize].clone();
                node.data_offsets[(i + 1) as usize] = node.data_offsets[i as usize];
                i -= 1;
            }
            node.keys[(i + 1) as usize] = key;
            node.data_offsets[(i + 1) as usize] = data_offset;
            self.save_node(&node);
        } else {
            while i >= 0 && key < node.keys[i as usize] { i -= 1; }
            let mut i = (i + 1) as usize;
            let child_offset = node.child_page_offsets[i];
            let child = self.load_node(child_offset);
            
            if child.keys.len() == (2 * T - 1) {
                self.split_child(&mut node, i);
                if key > node.keys[i] { i += 1; }
            }
            self.insert_non_full(node.child_page_offsets[i], key, data_offset);
        }
    }

    

    fn create_db(&self, name: &str) {
        let path = format!("{}.db", name);
        let mut file = File::create(path).unwrap();
        let mut page0 = vec![0u8; PAGE_SIZE];
        page0[0..4].copy_from_slice(b"RDBX");
        page0[8..16].copy_from_slice(&(PAGE_SIZE as u64).to_le_bytes());
        file.write_all(&page0).unwrap();

        let root = BTreeNode::new(PAGE_SIZE as u64, true);
        file.write_all(&root.serialize()).unwrap();
    }

    fn use_db(&mut self, name: &str) {
        let path = format!("{}.db", name);
        if !Path::new(&path).exists() { return; }
        let mut file = File::open(&path).unwrap();
        let mut header = [0u8; 16];
        file.read_exact(&mut header).unwrap();
        self.root_offset = u64::from_le_bytes(header[8..16].try_into().unwrap());
        self.active_name = Some(name.to_string());
        println!("Loaded v1 engine.");
    }

    fn set(&mut self, key: String, value: String) {
        if self.active_name.is_none() { return; }
        
        
        let mut file = OpenOptions::new().append(true).open(self.active_path()).unwrap();
        let data_pos = file.metadata().unwrap().len();
        file.write_all(&(key.len() as u32).to_le_bytes()).unwrap();
        file.write_all(&(value.len() as u32).to_le_bytes()).unwrap();
        file.write_all(key.as_bytes()).unwrap();
        file.write_all(value.as_bytes()).unwrap();

        
        let mut root = self.load_node(self.root_offset);
        if root.keys.len() == (2 * T - 1) {
            let new_root_offset = self.allocate_page();
            let mut new_root = BTreeNode::new(new_root_offset, false);
            new_root.child_page_offsets.push(self.root_offset);
            
            
            let mut file = OpenOptions::new().append(true).open(self.active_path()).unwrap();
            file.write_all(&[0u8; PAGE_SIZE]).unwrap();

            self.split_child(&mut new_root, 0);
            self.update_root_ptr(new_root_offset);
            self.insert_non_full(new_root_offset, key, data_pos);
        } else {
            self.insert_non_full(self.root_offset, key, data_pos);
        }
        println!("OK");
    }

    fn get(&self, key: &str) {
        if self.active_name.is_none() { return; }
        let mut curr_off = self.root_offset;
        loop {
            let node = self.load_node(curr_off);
            let mut i = 0;
            while i < node.keys.len() && key > &node.keys[i] { i += 1; }
            if i < node.keys.len() && key == &node.keys[i] {
                self.print_val(node.data_offsets[i]);
                return;
            }
            if node.is_leaf { println!("(nil)"); return; }
            curr_off = node.child_page_offsets[i];
        }
    }

    fn print_val(&self, offset: u64) {
        let mut file = File::open(self.active_path()).unwrap();
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

fn main() {
    let mut engine = DbEngine::new();
    println!("RustDB v1.0 - The Final Boss");
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
            "SET" => if parts.len() > 2 { engine.set(parts[1].to_string(), parts[2..].join(" ")) },
            "GET" => if parts.len() > 1 { engine.get(parts[1]) },
            "EXIT" => break,
            _ => println!("Unknown command."),
        }
    }
}