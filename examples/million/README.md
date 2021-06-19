# One Million Rows

This example adds one million rows to a collection, runs and measures a few different queries and transaction around it.

## Example output
```
running insert of 1000000 rows...                    
-> inserted 0 rows                            
-> inserted 100000 rows                       
-> inserted 200000 rows                       
-> inserted 300000 rows                       
-> inserted 400000 rows                       
-> inserted 500000 rows                       
-> inserted 600000 rows                       
-> inserted 700000 rows                       
-> inserted 800000 rows                       
-> inserted 900000 rows                       
-> insert took 4.5987582s                     
                                              
running full scan of age >= 30...             
-> result = 510000                            
-> full scan took 10.9725ms                   
                                              
running full scan of class == "rogue"...      
-> result = 358000                            
-> full scan took 10.9971ms                   
                                              
running indexed query of human mages...       
-> result = 68000                             
-> indexed query took 1.0026ms                
                                              
running indexed query of human female mages...
-> result = 32000                             
-> indexed query took 1.0348ms                
                                              
running update of balance of everyone...      
-> updated 1000000 rows                       
-> update took 65.995ms                       
                                              
running update of age of mages...             
-> updated 302000 rows                        
-> update took 11.9935ms                       
```