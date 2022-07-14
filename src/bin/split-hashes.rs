use std::{
    fs::{ OpenOptions, create_dir_all, File },
    io::{self, BufRead, Write, BufReader, SeekFrom, Seek},
    time::Instant,
    thread::{self, JoinHandle},
    str, env
};
use chrono::Duration;
use chrono_humanize::HumanTime;
use num_cpus;

// settings
const INPUT_FILE: &str = "./input.txt";
const OUTPUT_FOLDER: &str = "./output";
const READ_AHEAD:usize = 150;       // How far to read ahead from the block size for a newline (needs to be higher than max line length)

fn main() {
    let timer = Instant::now();
    let duration:Duration;

    // default to running on all logical processors (all cores/threads)
    let cores: u64 = num_cpus::get().try_into().unwrap();
    let mut concurrent_blocks = cores;
    // MAX 4GB RAM BY DEFAULT
    let mut max_memory = 4;

    let args: Vec<String> = env::args().collect();

    if args.len() >= 2 {
        max_memory = args[1].parse().expect("Invalid max memory usage");
    }
    if args.len() >= 3 {
        concurrent_blocks = args[2].parse().expect("Invalid max cores ");
    }

    let mut block_size = (max_memory * 1073741824) / concurrent_blocks;

    let file = File::open(INPUT_FILE).unwrap();
    let file_length: u64 = file.metadata().unwrap().len().try_into().unwrap();
    let file_length_mb: f64 = ((file_length / 1000000) as i32).try_into().unwrap();
    let block_size_mb: f64 = ((block_size as i64 / 1000000) as i32).try_into().unwrap();
    let concurrent_blocks_f64: f64 = ((concurrent_blocks as i64) as i32).try_into().unwrap();
    let block_count = (file_length_mb / block_size_mb).ceil();
    let last_round_discrepancy: f64 = (block_count % concurrent_blocks_f64) / concurrent_blocks_f64;

    // increase number of blocks if the last round of blocks is less than 80% efficient (this will reduce RAM usage and maximise CPU usage)
    if last_round_discrepancy != 0.0  && last_round_discrepancy < 0.80 {
        let new_block_count:u64  = ((block_count - (block_count % concurrent_blocks_f64) + concurrent_blocks_f64) as i32).try_into().unwrap();
        block_size = (file_length / new_block_count) + 1;
    }

    // prevent memory over provisioning from causing uneven distribution of work across cores 
    if (block_size * concurrent_blocks) > file_length {
        block_size = (file_length + 1) / concurrent_blocks;
    }

    println!("🔥 Running split with {} cores and {}MB RAM (block size {}MB)", concurrent_blocks, ((block_size * concurrent_blocks) / 1000000), block_size_mb);

    println!("🔥 Splitting file into blocks");
    let blocks = get_blocks(block_size);
    let total_blocks = blocks.len();

    println!("🔥 Splitting file by hash");
    let mut current_block = 0;

    while current_block < blocks.len() {
        let mut threads = vec![];

        for _ in 0..concurrent_blocks {
            if current_block < total_blocks {
                let thread = handle_block(blocks[current_block].to_owned());
                threads.push(thread);
                current_block += 1;
                println!("🔥 Now processing block {} of {}", current_block, total_blocks);
            }
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }

    duration = Duration::from_std(Instant::now().duration_since(timer)).unwrap();
    println!("🔥 File Split Complete {}", HumanTime::from(duration));
}


fn get_blocks(block_size: u64) -> Vec<Vec<usize>> {
    
    let mut file = File::open(INPUT_FILE).unwrap();
    let len: u64 = file.seek(SeekFrom::End(0)).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();

    let mut results: Vec<Vec<usize>> = vec![];
    let mut reader = BufReader::with_capacity(READ_AHEAD, file);

    let mut last_pos: u64 = 0;
    let mut block_count: u64 = 0;
    while last_pos < (len - block_size) {
        reader.seek_relative(block_size.try_into().unwrap()).unwrap();
        let buffer = reader.fill_buf().unwrap();

        let mut cursor = io::Cursor::new(buffer);
        let mut dummy_buf = Vec::new();
        cursor.read_until(b'\n', &mut dummy_buf).unwrap();

        let distance_to_newline = cursor.position();
        let current_block_length:u64 = block_size + distance_to_newline;
        let current_block_end = (block_count * block_size) + current_block_length;
        results.push(vec![last_pos.try_into().unwrap(), current_block_end.try_into().unwrap()]);

        block_count += 1;
        last_pos = current_block_end;
    }

    let last_block_start: usize = last_pos.try_into().unwrap();
    let last_block_end: usize = len.try_into().unwrap();
    results.push(vec![last_block_start, last_block_end]);
    
    return results; 
}


fn handle_block(block_range: Vec<usize>) -> JoinHandle<()> {
    return thread::spawn(move || {
        let buffer_size = block_range[1] - block_range[0];
        let file = File::open(INPUT_FILE).unwrap();
        let mut reader = BufReader::with_capacity(buffer_size, file);
        reader.seek_relative(block_range[0].try_into().unwrap()).unwrap();
        
        let buffer = reader.fill_buf().unwrap();
        let mut cursor = io::Cursor::new(buffer);

        let mut last_handle_id = [b'X', b'X', b'X', b'X'];
        let mut file_handle = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(".dummy_file")
            .unwrap();

        while cursor.position() < buffer.len().try_into().unwrap() {
            let mut line: Vec<u8> = Vec::new();
            cursor.read_until(b'\n', &mut line).unwrap();
            let full_prefix = &line[0..4];
            let hash_suffix = &line[4..40];

            if full_prefix != last_handle_id {
                last_handle_id = full_prefix.try_into().unwrap();
                let prefix1 =  str::from_utf8(&full_prefix[0..2]).unwrap();
                let prefix2 =  str::from_utf8(&full_prefix[2..4]).unwrap();
                let folder = format!("{}/{}", OUTPUT_FOLDER, prefix1);
                let file_to_open = format!("{}/{}", folder, prefix2);
    
                create_dir_all(folder).ok();
                file_handle = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(file_to_open)
                    .unwrap();
            }
                
            file_handle.write_all(hash_suffix).unwrap();
            file_handle.write_all("\n".as_bytes()).unwrap();
        }
    });
}