use std::{
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

    println!("🔥 Splitting file");
    let mmap = unsafe { Mmap::map(&file).expect("Error mapping file") };
    let mut reader: Box<dyn Read>;
    reader = Box::new(&mmap[..]);
    let buffer_reader = BufReader::with_capacity(1024 * 32 , reader.as_mut());
    
    let read_bar_style = ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>7}/{len:7} ({eta} left)").progress_chars("#>-");
    let read_bar = ProgressBar::new(lines as u64);
    read_bar.set_style(read_bar_style);


    let mut count = 0;
    let mut last_handle_id = "....".to_string();
    let mut file_handle = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(".dummy_file")
        .unwrap();

    for line in buffer_reader.lines() {

        let text = line.unwrap();
        let full_prefix = &text[0..4];
        let hash_suffix = &text[4..40];

        if full_prefix.to_string() != last_handle_id {
            last_handle_id = full_prefix.to_string();
            let prefix1 = full_prefix[0..2].to_string();
            let prefix2 = full_prefix[2..4].to_string();
            let folder = format!("{}/{}", OUTPUT_FOLDER, prefix1);
            let file_to_open = format!("{}/{}", folder, prefix2);

            create_dir_all(folder).ok();
            file_handle = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(file_to_open)
                .unwrap();
            // last_handle_id = text[0..4];

        }
        
        file_handle.write_all(hash_suffix.as_bytes()).unwrap();
        file_handle.write_all("\n".as_bytes()).unwrap();

        // update progress bar every 10000 lines
        count +=1;
        if count % 10000 == 0 { read_bar.inc(10000) }
    }
    
    read_bar.finish_and_clear();
    duration = Duration::from_std(Instant::now().duration_since(timer)).unwrap();
    println!("🔥 Hash File Split {}", HumanTime::from(duration));
}
