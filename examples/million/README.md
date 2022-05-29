# Ten Million Rows

This example adds 10 million rows to a collection, runs and measures a few different queries and transaction around it.

## Example output

```
running insert of 10000000 rows...
-> inserted 0 rows
...
-> inserted 9900000 rows
-> insert took 9.6700612s

running snapshot of 10000000 rows...
-> snapshot took 1.02656756s

running full scan of age >= 30...
-> result = 5100000
-> full scan took 26.217664ms

running full scan of class == "rogue"...
-> result = 3580000
-> full scan took 41.691512ms

running indexed query of human mages...
-> result = 680000
-> indexed query took 269.734µs

running indexed query of human female mages...
-> result = 320000
-> indexed query took 330.87µs

running update of balance of everyone...
-> updated 10000000 rows
-> update took 101.451402ms

running update of age of mages...
-> updated 3020000 rows
-> update took 39.874322ms
```
