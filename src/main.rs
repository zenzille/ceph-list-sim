use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};
use std::collections::VecDeque;
use std::time::Instant;
use clap::Parser;
use md5;

struct Bucket {
    shards: Vec<BTreeSet<String>>,
    rows_read: usize,
    queries: usize,
    calls: usize,
}

#[derive(Debug)]
struct ListResult {
    keys: VecDeque<String>,
    is_truncated: bool,
}

/*
 * Simulation of RGWRados::Bucket::List::list_objects_ordered
 * in src/rgw/driver/rados/rgw_rados.cc
 */
fn list(mut bucket: &mut Bucket, start: &str, delimiter: Option<char>,
    max_keys: usize, read_ahead: usize) -> (Vec<String>, bool)
{
    let mut result = Vec::new();
    let mut is_truncated;

    let to_read = max_keys.max(read_ahead);
    let mut last_key = start.to_string();

    if let Some(delimiter) = delimiter {
        if let Some(index) = last_key.find(delimiter) {
            last_key.truncate(index + 1);
            last_key.push(char::MAX);
        }
    }

    let mut previous_prefix = None;
    let mut attempt = 1;
    'outer: loop {
        is_truncated = false;
        let mut entries_to_request = to_read + 1 - attempt;
        entries_to_request = balls_into_bins(entries_to_request as f64,
            bucket.shards.len() as f64);
        entries_to_request = entries_to_request.max(8);
        println!("debug: attempt: {}, entries_to_request: {}, current results: {}",
            attempt, entries_to_request, result.len());

        /*
         * Request listing from all shards
         */
        let mut shard_res = Vec::new();
        for i in 0..bucket.shards.len() {
            let res = rgw_list(&mut bucket, i, &last_key, entries_to_request,
                delimiter);
            // if any of the shard lists is truncated, the result is truncated
            if res.is_truncated {
                is_truncated = true;
            }
            bucket.calls += 1;
            shard_res.push(res);
        }

        /*
         * Merge the results
         */
        'merge: loop {
            if result.len() >= max_keys {
                break 'outer;
            }
            // find lowest key index
            let mut min_key_index = None;
            for i in 0..shard_res.len() {
                let len = shard_res[i].keys.len();
                if len == 0 {
                    if shard_res[i].is_truncated {
                        break 'merge;
                    }
                    continue;
                }
                if let Some((min_key, _)) = min_key_index {
                    if &shard_res[i].keys[0] < min_key {
                        min_key_index = Some((&shard_res[i].keys[0], i));
                    }
                } else {
                    min_key_index = Some((&shard_res[i].keys[0], i));
                }
            }
            // de-duplicate results from different shards with the same
            // prefix if a delimiter is given
            if let Some((_, index)) = min_key_index {
                let key = shard_res[index].keys.pop_front().unwrap();
                if let Some(delimiter) = delimiter {
                    if let Some(pos) = key.find(delimiter) {
                        if let Some(prev) = &previous_prefix {
                            if key.starts_with(prev) {
                                continue;
                            }
                        }
                        let prefix = key[..pos+1].to_string();
                        previous_prefix = Some(prefix.clone());
                        result.push(prefix);
                    } else {
                        previous_prefix = None;
                    }
                } else {
                    result.push(key.clone());
                }
            } else {
                break;
            }
        }
        if delimiter.is_some() {
            if let Some(prev) = &previous_prefix {
                last_key = prev.clone();
                last_key.push(char::MAX);
            } else {
                panic!("unexpected, result is {:?}", result);
            }
        } else {
            last_key = result[result.len() - 1].clone();
        }
        // if we finished listing, or if we're returning at least half the
        // requested entries, that's enough; S3 and swift protocols allow
        // returning fewer than max entries
        if result.len() >= (max_keys + 1) / 2 {
            break;
        }
        attempt += 1;
        if attempt > 8 && result.len() >= 1 {
            break;
        }
    }

    (result, is_truncated)
}

/*
 * Simulation of rgw_bucket_list in src/cls/rgw/cls_rgw.cc
 *
 * This part runs on the OSDs
 */
fn rgw_list(bucket: &mut Bucket, shard_id: usize, start: &str,
    num_entries: usize, delimiter: Option<char>) -> ListResult
{
    let shard = &bucket.shards[shard_id];
    let mut result = VecDeque::new();
    let mut previous_prefix = None;
    let mut last_key = start.to_string();
    let mut is_truncated = false;
    'outer: for attempt in 1..=8 {
        let keys = shard.range::<str, _>((Excluded(last_key.as_str()), Unbounded))
            .take(num_entries + 1 - attempt)
            .collect::<Vec<_>>() ;
        if keys.len() == 0 {
            is_truncated = false;
            break;
        }
        is_truncated = keys[keys.len() - 1] != shard.last().unwrap();
        bucket.rows_read += keys.len();
        bucket.queries += 1;
        for &key in &keys {
            // de-duplicate results with the same prefix if a delimiter is given
            if let Some(delimiter) = delimiter {
                if let Some(pos) = key.find(delimiter) {
                    if let Some(prev) = &previous_prefix {
                        if key.starts_with(prev) {
                            continue;
                        }
                    }
                    let prefix = key[..pos+1].to_string();
                    previous_prefix = Some(prefix.clone());
                    result.push_back(prefix);
                } else {
                    previous_prefix = None;
                    result.push_back(key.clone());
                }
            } else {
                result.push_back(key.clone());
            }
            if result.len() >= num_entries {
                break 'outer;
            }
        }
        if let (Some(_), Some(prev)) = (&delimiter, &previous_prefix) {
            last_key = prev.clone();
            last_key.push(char::MAX);

        } else {
            last_key = keys[keys.len() - 1].to_string();
        }
    }

    ListResult {
        keys: result,
        is_truncated,
    }
}

/*
 * The following is based on _"Balls into Bins" -- A Simple and                
 * Tight Analysis_ by Raab and Steger.
 * See RGWRados::calc_ordered_bucket_list_per_shard
 */
fn balls_into_bins(num_entries: f64, num_shards: f64) -> usize {
    let bib =  1. + (num_entries / num_shards) +
          ((2. * num_entries) * num_shards.ln() / num_shards).sqrt();
    bib as usize
}

fn new_bucket(num_shards: usize) -> Bucket {
    Bucket {
        rows_read: 0,
        queries: 0,
        calls: 0,
        shards: vec![BTreeSet::new(); num_shards],
    }
}

fn add_to_bucket(bucket: &mut Bucket, object: &str) {
    let digest = md5::compute(object);
    let mut v = 0;
    for b in digest.iter() {
        v = (v << 8) | *b as u128;
    }
    let nshards = bucket.shards.len() as u128;
    bucket.shards[(v % nshards) as usize].insert(object.to_string());
}

fn create_bucket(num_shards: usize, num_dirs: usize, num_entries: usize)
    -> Bucket
{
    let mut bucket = new_bucket(num_shards);
    let mut now = Instant::now();
    for dir in 0..num_dirs {
        for file in 0..num_entries {
            let object = format!("dir{:02}/file{:06}", dir, file);
            add_to_bucket(&mut bucket, &object);
            if file % 10000 == 0 {
                if now.elapsed().as_secs() > 1 {
                    println!("adding {}", object);
                    now = Instant::now();
                }
            }
        }
    }

    bucket
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short='s', long, default_value_t=11)]
    shards: usize,

    #[arg(short='d', long, default_value_t=30)]
    dirs: usize,

    #[arg(short='e', long, default_value_t=100000)]
    entries: usize,

    #[arg(short='l', long, default_value_t=false)]
    delimiter: bool,

    #[arg(short='m', long, default_value_t=10)]
    max_keys: usize,

    #[arg(short='r', long, default_value_t=1000)]
    read_ahead: usize,
}

fn main() {
    let cli = Cli::parse();

    println!("creating bucket with {} shards, {} dirs with {} entries each",
        cli.shards, cli.dirs, cli.entries);
    let mut bucket = create_bucket(cli.shards, cli.dirs, cli.entries);
    let delimiter = cli.delimiter.then(|| '/');
    let delim_str = if delimiter.is_some() {
        " with delimiter /"
    } else {
        ""
    };

    let mut total_fetched = 0;
    let mut s3_calls = 0;
    loop {
        s3_calls += 1;
        println!("listing bucket with max_keys: {}, read_ahead: {}{}",
            cli.max_keys - total_fetched, cli.read_ahead, delim_str);
        let res = list(&mut bucket, "", delimiter,
            cli.max_keys - total_fetched, cli.read_ahead);
        println!("list returned {} entries, truncated is {}", res.0.len(),
            res.1.to_string());
        total_fetched += res.0.len();
        if total_fetched >= cli.max_keys || res.1 == false {
            break;
        }
    }
    println!("");
    println!("{} requests to S3 were made", s3_calls);
    println!("{} requests to OSDs were made", bucket.calls);
    println!("{} database queries were submitted", bucket.queries);
    println!("{} rows were returned", bucket.rows_read);
}

#[cfg (test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        // test rgw_list
        let mut bucket = create_bucket(11, 30, 100000);
        let res = rgw_list(&mut bucket, 0, "", 10, None);
        assert!(res.keys.len() == 10);
        assert_eq!(res.is_truncated, true);
        let res = rgw_list(&mut bucket, 0, "", 10, Some('/'));
        println!("{:?}", res);
        println!("rows_read: {}", bucket.rows_read);
        println!("queries: {}", bucket.queries);
        println!("calls: {}", bucket.calls);
        assert!(res.keys.len() == 8);
        assert_eq!(res.is_truncated, true);

        // test list
        let res = list(&mut bucket, "", None, 100, 100);
        assert!(res.0.len() == 100);
        assert_eq!(res.1, true);

        bucket.rows_read = 0; 
        bucket.queries = 0; 
        bucket.calls = 0; 
        let res = list(&mut bucket, "", Some('/'), 100, 1000);
        println!("{:?}", res);
        println!("rows_read: {}", bucket.rows_read);
        println!("queries: {}", bucket.queries);
        println!("calls: {}", bucket.calls);

        let mut bucket = create_bucket(11, 1, 100);
        let res = list(&mut bucket, "", None, 100, 100);
        assert!(res.0.len() == 100);
        assert_eq!(res.1, false);

        let res = list(&mut bucket, "", Some('/'), 100, 1000);
        assert!(res.0.len() == 1);
        assert_eq!(res.1, false);

    }
}
