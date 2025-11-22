1. Describe in a few sentences what you would change if you knew it would be used for a 10GB CSV input file.
   - I would process csv rows in chuncks and transactions rather than loading entire table into memory.
   - Process dediblication using sql query rather than c# tools.
   - Indexes to optimize queries and search.
   - Careful parallelization together with batches would significantly speed up the process. 
2. Number of rows in your table after running the program.
   - 29889
