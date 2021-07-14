# Concurrency Benchmark

This is an example benchmark with various workloads (90% read / 10% write, etc) on a collection of 1 million elements with different goroutine pools. In this example we're combining two types of transactions:
 * Read transactions that query a random index and iterate over the results over a single column.
 * Write transactions that update a random element (point-write).

Note that the goal of this benchmark is to validater concurrency, not throughput this represents the current "best" case scenario when the updates are random and do less likely to incur contention. Reads, however quite often would hit the same chunks as only the index itself is randomized.

```
90%-10%       1 procs      143,221,213 read/s         70 write/s
90%-10%       8 procs    1,081,511,102 read/s        483 write/s
90%-10%      16 procs    1,068,562,727 read/s        455 write/s
90%-10%      32 procs    1,042,382,561 read/s        442 write/s
90%-10%      64 procs    1,039,644,346 read/s        446 write/s
90%-10%     128 procs    1,049,228,432 read/s        442 write/s
90%-10%     256 procs    1,027,362,194 read/s        477 write/s
90%-10%     512 procs    1,023,097,576 read/s        457 write/s
90%-10%    1024 procs      996,585,722 read/s        436 write/s
90%-10%    2048 procs      948,455,719 read/s        494 write/s
90%-10%    4096 procs      930,094,338 read/s        540 write/s
50%-50%       1 procs      142,015,047 read/s        598 write/s
50%-50%       8 procs    1,066,028,881 read/s      4,300 write/s
50%-50%      16 procs    1,039,210,987 read/s      4,191 write/s
50%-50%      32 procs    1,042,789,993 read/s      4,123 write/s
50%-50%      64 procs    1,040,410,050 read/s      4,102 write/s
50%-50%     128 procs    1,006,464,963 read/s      4,008 write/s
50%-50%     256 procs    1,008,663,071 read/s      4,170 write/s
50%-50%     512 procs      989,864,228 read/s      4,146 write/s
50%-50%    1024 procs      998,826,089 read/s      4,258 write/s
50%-50%    2048 procs      939,110,917 read/s      4,515 write/s
50%-50%    4096 procs      866,137,428 read/s      5,291 write/s
10%-90%       1 procs      135,493,165 read/s      4,968 write/s
10%-90%       8 procs    1,017,928,553 read/s     37,130 write/s
10%-90%      16 procs    1,040,251,193 read/s     37,521 write/s
10%-90%      32 procs      982,115,784 read/s     35,689 write/s
10%-90%      64 procs      975,158,264 read/s     34,041 write/s
10%-90%     128 procs      940,466,888 read/s     34,827 write/s
10%-90%     256 procs      930,871,315 read/s     34,399 write/s
10%-90%     512 procs      892,502,438 read/s     33,955 write/s
10%-90%    1024 procs      834,594,229 read/s     32,953 write/s
10%-90%    2048 procs      785,583,770 read/s     32,882 write/s
10%-90%    4096 procs      688,402,474 read/s     34,646 write/s
```