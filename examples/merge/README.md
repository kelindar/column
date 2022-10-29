# Custom merging strategy

In this example we are creating a column called `location` that contains a JSON-encoded position and velocity. The position is updated by calling a `MergeString()` function with the velocity vector, the updates are then merged atomically using a merging function specified.

The merge happens when transaction is committed to ensure consistency. Hence, this technique allows for two concurrent transactions to update the same position.

## Example output

```json
00: {"position":[1,2],"velocity":[1,2]}
01: {"position":[2,4],"velocity":[1,2]}
02: {"position":[3,6],"velocity":[1,2]}
03: {"position":[4,8],"velocity":[1,2]}
04: {"position":[5,10],"velocity":[1,2]}
05: {"position":[6,12],"velocity":[1,2]}
06: {"position":[7,14],"velocity":[1,2]}
07: {"position":[8,16],"velocity":[1,2]}
08: {"position":[9,18],"velocity":[1,2]}
09: {"position":[10,20],"velocity":[1,2]}
10: {"position":[11,22],"velocity":[1,2]}
11: {"position":[12,24],"velocity":[1,2]}
12: {"position":[13,26],"velocity":[1,2]}
13: {"position":[14,28],"velocity":[1,2]}
14: {"position":[15,30],"velocity":[1,2]}
15: {"position":[16,32],"velocity":[1,2]}
16: {"position":[17,34],"velocity":[1,2]}
17: {"position":[18,36],"velocity":[1,2]}
18: {"position":[19,38],"velocity":[1,2]}
19: {"position":[20,40],"velocity":[1,2]}
```
