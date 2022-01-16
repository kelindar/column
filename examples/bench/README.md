# Concurrency Benchmark

This is an example benchmark with various workloads (90% read / 10% write, etc) on a collection of 1 million elements with different goroutine pools. In this example we're combining two types of transactions:

- Read transactions that update a random element (point-read).
- Write transactions that update a random element (point-write).

Note that the goal of this benchmark is to validate concurrency, not throughput this represents the current "best" case scenario when the updates are random and do less likely to incur contention. Reads, however quite often would hit the same chunks as only the index itself is randomized.

## Results

Below are some results from running on my 8-core machine (Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz).

```
   WORK  PROCS          READ RATE          WRITE RATE
100%-0%      1    6,080,402 txn/s               0 txn/s
100%-0%      2   11,280,415 txn/s               0 txn/s
100%-0%      4   23,909,267 txn/s               0 txn/s
100%-0%      8   44,142,401 txn/s               0 txn/s
100%-0%     16   43,839,560 txn/s               0 txn/s
100%-0%     32   45,981,323 txn/s               0 txn/s
100%-0%     64   42,550,034 txn/s               0 txn/s
100%-0%    128   41,748,237 txn/s               0 txn/s
100%-0%    256   42,838,515 txn/s               0 txn/s
100%-0%    512   44,023,907 txn/s               0 txn/s
90%-10%      1    5,275,465 txn/s         582,720 txn/s
90%-10%      2    7,739,053 txn/s         895,427 txn/s
90%-10%      4    9,355,436 txn/s       1,015,179 txn/s
90%-10%      8    8,605,764 txn/s         972,278 txn/s
90%-10%     16   10,254,677 txn/s       1,138,855 txn/s
90%-10%     32   10,231,753 txn/s       1,146,337 txn/s
90%-10%     64   10,708,470 txn/s       1,190,486 txn/s
90%-10%    128    9,863,114 txn/s       1,111,391 txn/s
90%-10%    256    9,149,044 txn/s       1,008,791 txn/s
90%-10%    512    9,131,921 txn/s       1,017,933 txn/s
50%-50%      1    2,308,520 txn/s       2,323,510 txn/s
50%-50%      2    2,387,979 txn/s       2,370,993 txn/s
50%-50%      4    2,381,743 txn/s       2,321,850 txn/s
50%-50%      8    2,250,533 txn/s       2,293,409 txn/s
50%-50%     16    2,272,368 txn/s       2,272,368 txn/s
50%-50%     32    2,181,658 txn/s       2,268,687 txn/s
50%-50%     64    2,245,193 txn/s       2,228,612 txn/s
50%-50%    128    2,172,485 txn/s       2,124,144 txn/s
50%-50%    256    1,871,648 txn/s       1,830,572 txn/s
50%-50%    512    1,489,572 txn/s       1,525,730 txn/s
10%-90%      1      383,770 txn/s       3,350,996 txn/s
10%-90%      2      318,691 txn/s       2,969,129 txn/s
10%-90%      4      316,425 txn/s       2,826,869 txn/s
10%-90%      8      341,467 txn/s       2,751,654 txn/s
10%-90%     16      300,528 txn/s       2,861,470 txn/s
10%-90%     32      349,121 txn/s       2,932,224 txn/s
10%-90%     64      344,824 txn/s       2,869,017 txn/s
10%-90%    128      287,559 txn/s       2,718,741 txn/s
10%-90%    256      253,480 txn/s       2,366,967 txn/s
10%-90%    512      220,717 txn/s       2,102,277 txn/s
0%-100%      1            0 txn/s       3,601,751 txn/s
0%-100%      2            0 txn/s       3,054,833 txn/s
0%-100%      4            0 txn/s       3,171,539 txn/s
0%-100%      8            0 txn/s       2,962,326 txn/s
0%-100%     16            0 txn/s       2,986,498 txn/s
0%-100%     32            0 txn/s       3,068,877 txn/s
0%-100%     64            0 txn/s       2,994,055 txn/s
0%-100%    128            0 txn/s       2,802,362 txn/s
0%-100%    256            0 txn/s       2,444,133 txn/s
0%-100%    512            0 txn/s       2,180,372 txn/s
```
