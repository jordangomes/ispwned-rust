extern crate keepass;
use keepass::{Database, NodeRef};
use std::{
    env,
    fs::File,
    io::{BufRead},
    time::{ Instant }
};
use crypto::{
    digest::Digest,
    sha1::Sha1
};

fn main() {
    let args: Vec<String> = env::args().collect();
    let task = args[1].to_string();
    let source = args[2].to_string();

    let now = Instant::now();
    if task == "-f" {
        let (found, lines) = check_passwords_from_file(&source, false);
        println!("Found {}/{} passwords in {} ms", found, lines, now.elapsed().as_millis())
    } else if task == "-k" {
        let password = args[3].to_string();
        check_password_from_keepass_db(&source, &password)
    } else {
        if check_password(&source, true) {           
            println!("Password: {} found in {} ms", source, now.elapsed().as_millis())
        } else {
            println!("Password: {} not found in {} ms", source, now.elapsed().as_millis())
        }
    }

}

fn check_passwords_from_file(password_file_path:&String, log:bool) -> (i32, i32) {
    let file = File::open(password_file_path).expect("Error opening file");
    
    let mut lines = 0;
    let mut found = 0;

    for line in std::io::BufReader::new(file).lines() {
        let password = line.unwrap();
        if check_password(&password, false) {
            if log { 
                println!("Password: {} found", password);
             }
            found += 1
        } else if log {
            println!("Password: {} not found", password); 
        }
        lines += 1
    }

    return (found, lines)
}

fn check_password_from_keepass_db(db:&String, password:&String) {
    let path = std::path::Path::new(db);
    let db = Database::open(&mut File::open(path).unwrap(), Some(password), None).unwrap();

    // Iterate over all Groups and Nodes
    for node in &db.root {
        match node {
            NodeRef::Group(_g) => {
            },
            NodeRef::Entry(e) => {
                let title = e.get_title().unwrap();
                let user = e.get_username().unwrap();
                let pass = e.get_password().unwrap();
                if check_password(&pass.to_string(), false) {
                    println!("Compromised Entry '{0}': '{1}'", title, user);
                }
            }
        }
    }
}

fn check_password(password:&String, log:bool) -> bool {
    let hashed_password = hash_password(password.to_string());
    let hash_prefix1 = &hashed_password[0..2];
    let hash_prefix2 = &hashed_password[2..4];
    let hash_suffix = &hashed_password[4..40];

    let hash_file_path = format!("./output/{}/{}", hash_prefix1, hash_prefix2);

    if log {
        println!("Searching for hash suffix {:?} in {:?}", hash_suffix, hash_file_path);
    }

    let file = File::open(hash_file_path).expect("Error opening file");

    for line in std::io::BufReader::new(file).lines() {
        let suffix = line.unwrap();
        if suffix == hash_suffix {
            return true
        }
    }
    return false
}

fn hash_password(password:String) -> String {
    let mut hasher = Sha1::new();
    hasher.input_str(&password);
    let result = hasher.result_str().to_ascii_uppercase();
    return result
}