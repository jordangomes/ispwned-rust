use std::{
    collections::HashMap,
    fs::{ File, OpenOptions, create_dir_all },
    io::{ Write, Read, BufReader, BufRead },
    time::Instant
};
use memmap::Mmap;
use linecount::count_lines;
use chrono::Duration;
use chrono_humanize::HumanTime;
use indicatif::{ProgressBar, ProgressStyle};

// settings
const INPUT_FILE: &str = "./input.txt";
const OUTPUT_FOLDER: &str = "./output";

const PREFIX1_LENGTH:u32 = 2;
const PREFIX2_LENGTH:u32 = 2;

// calculated settings
const FULL_PREFIX_LENGTH:u32 = PREFIX1_LENGTH + PREFIX2_LENGTH;
const MAX_BYTE_VALUE:u32 = 16;
const MAX_PREFIX_VALUE:u32 = MAX_BYTE_VALUE.pow(FULL_PREFIX_LENGTH);

fn main() {
    let mut timer = Instant::now();
    let mut duration:Duration;

    // open file and get linecount
    let file = File::open(INPUT_FILE).unwrap();
    println!("🔥 Getting file size - this might take a while...");
    let lines = count_lines(&file).unwrap();
    duration = Duration::from_std(Instant::now().duration_since(timer)).unwrap();
    println!("🔥 File Size Calulated in {}", HumanTime::from(duration));
    
    timer = Instant::now();

    // create folders and open file handles
    println!("🔥 Opening {:?} File Handles", MAX_PREFIX_VALUE);
    let fh_bar = ProgressBar::new(MAX_PREFIX_VALUE as u64);
    let fh_bar_style = ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>7}/{len:7} ({eta} left)").progress_chars("#>-");
    fh_bar.set_style(fh_bar_style);
    let mut file_handles:HashMap<String, File> = HashMap::new();
    for i in 0..MAX_PREFIX_VALUE {
        if i % 1000 == 0 { fh_bar.inc(1000) };
        let hex = format!("{:#06x}", i).to_uppercase();
        let prefix1 = &hex[2..2+PREFIX1_LENGTH as usize].to_string();
        let prefix2 = &hex[2+PREFIX1_LENGTH as usize..2+PREFIX1_LENGTH as usize+PREFIX2_LENGTH as usize].to_string();
        let handle_id = format!("{}{}",prefix1, prefix2);
        let folder = format!("{}/{}", OUTPUT_FOLDER, prefix1);
        let file_to_open = format!("{}/{}", folder, prefix2);

        create_dir_all(folder).ok();
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_to_open)
            .unwrap();

        file_handles.insert(handle_id, file);
    }
    fh_bar.finish_and_clear();
    duration = Duration::from_std(Instant::now().duration_since(timer)).unwrap();
    println!("🔥 File Handles Opened in {}", HumanTime::from(duration));

    timer = Instant::now();

    println!("🔥 Splitting file");
    let mmap = unsafe { Mmap::map(&file).expect("Error mapping file") };
    let mut reader: Box<dyn Read>;
    reader = Box::new(&mmap[..]);
    let buffer_reader = BufReader::with_capacity(1024 * 32 , reader.as_mut());
    let read_bar_style = ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>7}/{len:7} ({eta} left)").progress_chars("#>-");
    let read_bar = ProgressBar::new(lines as u64);
    read_bar.set_style(read_bar_style);
    let mut count = 0;
    for line in buffer_reader.lines() {
        count +=1;
        let text = line.unwrap();
        let handle_id = &text[0..4];
        let hash_suffix = &text[4..40];
        let mut file_handle = file_handles.get(handle_id).expect(&format!("Couldn't get handle: {:?}", handle_id));
        file_handle.write_all(hash_suffix.as_bytes()).unwrap();
        file_handle.write_all("\n".as_bytes()).unwrap();
        if count % 10000 == 0 { read_bar.inc(10000) }
    }
    read_bar.finish_and_clear();
    duration = Duration::from_std(Instant::now().duration_since(timer)).unwrap();
    println!("🔥 Hash File Split {}", HumanTime::from(duration));
}
