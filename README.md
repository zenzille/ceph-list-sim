# ceph-list-sim, a tool to simulate internal load of list queries

## Building
At least rust 1.74.0 is needed, build with cargo build --release.

## Usage

ceph-list-sim first creates a bucket with a given number of directories. Each directory
contains the same number of files. An example object would be `dir16/file731739`.
You can also configure the number of shards to simulate.

When querying, you can choose the max-keys to fetch, and whether to use a delimiter (`/`)
or request a full listing.

Another parameter is the read-ahead, which in ceph can be configured with
```
ceph config set client.rgw rgw_list_bucket_min_readahead 1000
```

Internally, ceph list roughly works like this:
* Rgw receives the list call,
* Rgw queries the OSD for each shard for a partial list of the result,
* The OSD sends a query to the local RocksDB,
* The RocksDB returns a number of rows,
* The OSDs as well as the rgw query in loops until enough records are collected to satisfy the request.

As output, ceph-list-sim reports the total number of calls made to s3 to collect
max-keys, the total number of queries the rgw made to the OSD, the total number
of database queries the OSDs made and the total number of rows returned by the databases.
See below for some example outputs.

```
Usage: ceph-list-sim [OPTIONS]

Options:
  -s, --shards <SHARDS>          [default: 11]
  -d, --dirs <DIRS>              [default: 30]
  -e, --entries <ENTRIES>        [default: 100000]
  -d, --delimiter                
  -m, --max-keys <MAX_KEYS>      [default: 10]
  -r, --read-ahead <READ_AHEAD>  [default: 1000]
  -h, --help                     Print help
  -V, --version                  Print version
```

## Examples

```
target/release/ceph-list-sim --dirs 100 -e 1000000 -s 11 --delimiter -m 100
...
4 requests to S3 were made
143 requests to OSDs were made
1144 database queries were submitted
124124 rows were returned
```
```
target/release/ceph-list-sim --dirs 100 -e 1000000 -s 11 --delimiter -m 100 -r 10
...
4 requests to S3 were made
143 requests to OSDs were made
1144 database queries were submitted
10252 rows were returned
```
```
target/release/ceph-list-sim --dirs 100 -e 1000000 -s 59 --delimiter -m 100
...
4 requests to S3 were made
767 requests to OSDs were made
6136 database queries were submitted
156468 rows were returned
```
```
target/release/ceph-list-sim --dirs 100 -e 1000000 -s 1999 --delimiter -m 100
...
4 requests to S3 were made
25987 requests to OSDs were made
207896 database queries were submitted
935532 rows were returned
```
```
target/release/ceph-list-sim --dirs 100 -e 1000000 -s 16411 --delimiter -m 100
...
4 requests to S3 were made
213343 requests to OSDs were made
1706744 database queries were submitted
7680348 rows were returned
```
```
target/release/ceph-list-sim --dirs 100 -e 1000000 -s 16411 --delimiter -m 100 -r 10
...
4 requests to S3 were made
213343 requests to OSDs were made
1706744 database queries were submitted
7680348 rows were returned
```
```
target/release/ceph-list-sim --dirs 100 -e 1000000 -s 16411 -m 100 
...
1 requests to S3 were made
16411 requests to OSDs were made
16411 database queries were submitted
131288 rows were returned
```
