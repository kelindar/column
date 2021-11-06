# Concurrency Benchmark

This is an example benchmark with various workloads (90% read / 10% write, etc) on a collection of 1 million elements with different goroutine pools. In this example we're combining two types of transactions:

- Read transactions that update a random element (point-read).
- Write transactions that update a random element (point-write).

Note that the goal of this benchmark is to validate concurrency, not throughput this represents the current "best" case scenario when the updates are random and do less likely to incur contention. Reads, however quite often would hit the same chunks as only the index itself is randomized.

## Results

Below are some results from running on my 8-core machine (Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz).

```
   WORK  PROCS          READ RATE          WRITE RATE
100%-0%      1    8,877,887 txn/s             0 txn/s
100%-0%      2   15,898,759 txn/s             0 txn/s
100%-0%      4   30,186,227 txn/s             0 txn/s
100%-0%      8   60,411,415 txn/s             0 txn/s
100%-0%     16   60,562,479 txn/s             0 txn/s
100%-0%     32   61,969,664 txn/s             0 txn/s
100%-0%     64   61,116,153 txn/s             0 txn/s
100%-0%    128   61,273,966 txn/s             0 txn/s
100%-0%    256   62,303,786 txn/s             0 txn/s
100%-0%    512   62,162,812 txn/s             0 txn/s
90%-10%      1    2,007,549 txn/s       223,615 txn/s
90%-10%      2    2,405,165 txn/s       252,705 txn/s
90%-10%      4    2,375,443 txn/s       255,679 txn/s
90%-10%      8    2,332,451 txn/s       234,237 txn/s
90%-10%     16    2,002,032 txn/s       218,043 txn/s
90%-10%     32    2,264,347 txn/s       201,639 txn/s
90%-10%     64    1,491,475 txn/s       181,956 txn/s
90%-10%    128    1,537,664 txn/s       180,435 txn/s
90%-10%    256    1,565,039 txn/s       157,420 txn/s
90%-10%    512    1,241,398 txn/s       124,654 txn/s
50%-50%      1      285,995 txn/s       298,950 txn/s
50%-50%      2      279,422 txn/s       287,377 txn/s
50%-50%      4      298,716 txn/s       265,197 txn/s
50%-50%      8      258,017 txn/s       250,169 txn/s
50%-50%     16      267,412 txn/s       238,427 txn/s
50%-50%     32      217,380 txn/s       201,791 txn/s
50%-50%     64      161,592 txn/s       178,441 txn/s
50%-50%    128      156,302 txn/s       147,838 txn/s
50%-50%    256       98,375 txn/s       114,311 txn/s
50%-50%    512      104,266 txn/s        96,785 txn/s
10%-90%      1       36,726 txn/s       315,646 txn/s
10%-90%      2       25,663 txn/s       244,789 txn/s
10%-90%      4       31,266 txn/s       234,497 txn/s
10%-90%      8       24,672 txn/s       221,105 txn/s
10%-90%     16       22,289 txn/s       205,061 txn/s
10%-90%     32       16,630 txn/s       188,473 txn/s
10%-90%     64       21,779 txn/s       216,389 txn/s
10%-90%    128       19,997 txn/s       164,261 txn/s
10%-90%    256       12,962 txn/s       109,386 txn/s
10%-90%    512       10,434 txn/s        93,333 txn/s
0%-100%      1            0 txn/s       313,133 txn/s
0%-100%      2            0 txn/s       239,831 txn/s
0%-100%      4            0 txn/s       231,702 txn/s
0%-100%      8            0 txn/s       218,349 txn/s
0%-100%     16            0 txn/s       204,190 txn/s
0%-100%     32            0 txn/s       192,038 txn/s
0%-100%     64            0 txn/s       173,347 txn/s
0%-100%    128            0 txn/s       138,415 txn/s
0%-100%    256            0 txn/s       105,254 txn/s
0%-100%    512            0 txn/s        93,103 txn/s
```
