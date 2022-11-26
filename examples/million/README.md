# Ten Million Rows

This example adds 10 million rows to a collection, runs and measures a few different queries and transaction around it.

## Example output

```
running insert of 10000000 rows...
-> insert took 7.6464726s

running snapshot of 10000000 rows...
-> snapshot took 1.35868707s

running full scan of age >= 30...
-> result = 5100000
-> full scan took 27.011615ms

running full scan of class == "rogue"...
-> result = 3580000
-> full scan took 45.053185ms

running indexed query of human mages...
-> result = 680000
-> indexed query took 309.74µs

running indexed query of human female mages...
-> result = 320000
-> indexed query took 385.606µs

running update of balance of everyone...
-> updated 10000000 rows
-> update took 108.09756ms

running update of age of mages...
-> updated 3020000 rows
-> update took 41.731165ms
```
