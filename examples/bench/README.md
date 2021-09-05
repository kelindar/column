# Concurrency Benchmark

This is an example benchmark with various workloads (90% read / 10% write, etc) on a collection of 1 million elements with different goroutine pools. In this example we're combining two types of transactions:
 * Read transactions that query a random index and iterate over the results over a single column.
 * Write transactions that update a random element (point-write).

Note that the goal of this benchmark is to validater concurrency, not throughput this represents the current "best" case scenario when the updates are random and do less likely to incur contention. Reads, however quite often would hit the same chunks as only the index itself is randomized.

```
90%-10%       1 procs      249,208,310 read/s        117 write/s
90%-10%       8 procs    1,692,667,386 read/s        738 write/s
90%-10%      16 procs    1,509,926,215 read/s        635 write/s
90%-10%      32 procs    1,489,456,934 read/s        660 write/s
90%-10%      64 procs    1,533,053,898 read/s        666 write/s
90%-10%     128 procs    1,495,078,423 read/s        654 write/s
90%-10%     256 procs    1,443,437,689 read/s        656 write/s
90%-10%     512 procs    1,464,321,958 read/s        704 write/s
90%-10%    1024 procs    1,495,877,020 read/s        635 write/s
90%-10%    2048 procs    1,413,233,904 read/s        658 write/s
90%-10%    4096 procs    1,376,644,077 read/s        743 write/s
50%-50%       1 procs      236,528,133 read/s        861 write/s
50%-50%       8 procs    1,589,501,618 read/s      6,335 write/s
50%-50%      16 procs    1,607,166,585 read/s      6,484 write/s
50%-50%      32 procs    1,575,200,925 read/s      6,438 write/s
50%-50%      64 procs    1,432,978,587 read/s      5,808 write/s
50%-50%     128 procs    1,181,986,760 read/s      4,606 write/s
50%-50%     256 procs    1,529,174,062 read/s      6,180 write/s
50%-50%     512 procs    1,472,102,974 read/s      5,961 write/s
50%-50%    1024 procs    1,399,040,792 read/s      6,066 write/s
50%-50%    2048 procs    1,295,570,830 read/s      5,919 write/s
50%-50%    4096 procs    1,181,556,697 read/s      5,871 write/s
10%-90%       1 procs      199,670,671 read/s      7,119 write/s
10%-90%       8 procs    1,224,172,050 read/s     44,464 write/s
10%-90%      16 procs    1,317,755,536 read/s     46,451 write/s
10%-90%      32 procs    1,429,807,620 read/s     51,758 write/s
10%-90%      64 procs    1,413,067,976 read/s     51,304 write/s
10%-90%     128 procs    1,302,410,992 read/s     46,375 write/s
10%-90%     256 procs    1,223,553,655 read/s     45,110 write/s
10%-90%     512 procs    1,120,740,609 read/s     42,799 write/s
10%-90%    1024 procs    1,071,064,037 read/s     41,519 write/s
10%-90%    2048 procs    1,044,805,034 read/s     42,868 write/s
10%-90%    4096 procs      877,312,822 read/s     42,910 write/s
```