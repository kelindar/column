# Concurrency Benchmark

This is an example benchmark with various workloads (90% read / 10% write, etc) on a collection of 1 million elements with different goroutine pools. In this example we're combining two types of transactions:
 * Read transactions that update a random element (point-read).
 * Write transactions that update a random element (point-write).

Note that the goal of this benchmark is to validate concurrency, not throughput this represents the current "best" case scenario when the updates are random and do less likely to incur contention. Reads, however quite often would hit the same chunks as only the index itself is randomized.

```
   WORK         PROCS              READS             WRITES
90%-10%       1 procs       51,642 txn/s        5,884 txn/s
90%-10%       8 procs      195,201 txn/s       21,803 txn/s
90%-10%      16 procs      311,078 txn/s       34,519 txn/s
90%-10%      32 procs      370,100 txn/s       41,225 txn/s
90%-10%      64 procs      374,964 txn/s       41,582 txn/s
90%-10%     128 procs      347,933 txn/s       38,589 txn/s
90%-10%     256 procs      337,840 txn/s       37,329 txn/s
90%-10%     512 procs      342,272 txn/s       37,692 txn/s
90%-10%    1024 procs      339,367 txn/s       37,049 txn/s
90%-10%    2048 procs      327,060 txn/s       35,568 txn/s
90%-10%    4096 procs      314,160 txn/s       32,818 txn/s
50%-50%       1 procs       28,944 txn/s       29,054 txn/s
50%-50%       8 procs       59,487 txn/s       59,342 txn/s
50%-50%      16 procs       70,271 txn/s       70,276 txn/s
50%-50%      32 procs       70,067 txn/s       69,796 txn/s
50%-50%      64 procs       61,443 txn/s       61,559 txn/s
50%-50%     128 procs       54,985 txn/s       54,760 txn/s
50%-50%     256 procs       53,684 txn/s       53,465 txn/s
50%-50%     512 procs       62,488 txn/s       61,967 txn/s
50%-50%    1024 procs       69,211 txn/s       68,090 txn/s
50%-50%    2048 procs       74,262 txn/s       73,639 txn/s
50%-50%    4096 procs       77,700 txn/s       75,452 txn/s
10%-90%       1 procs        4,811 txn/s       43,825 txn/s
10%-90%       8 procs        8,585 txn/s       77,136 txn/s
10%-90%      16 procs        8,582 txn/s       77,260 txn/s
10%-90%      32 procs        8,866 txn/s       79,127 txn/s
10%-90%      64 procs        8,090 txn/s       73,265 txn/s
10%-90%     128 procs        7,412 txn/s       67,985 txn/s
10%-90%     256 procs        6,473 txn/s       58,903 txn/s
10%-90%     512 procs        6,916 txn/s       61,835 txn/s
10%-90%    1024 procs        7,989 txn/s       71,794 txn/s
10%-90%    2048 procs        8,930 txn/s       78,657 txn/s
10%-90%    4096 procs        9,231 txn/s       81,465 txn/s
```