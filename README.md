# ceph-list-sim, a tool to simulate internal load of list queries

## Building
At least rust 1.74.0 is needed, build with cargo build --release.

## Usage
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
