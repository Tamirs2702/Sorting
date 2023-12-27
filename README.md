Single Thread:
Time Complexity:  O(n log n), where n is the number of records in the file. 
Space Complexity: The space complexity is O(x), where x is the maximum number of records in memory, as we ensure not to exceed this limit during sorting and merging.

Multi Threaded: 
Time Complexity:  With perfect scaling, using p threads could reduce time complexity to approximately O(n log n / p) in best-case scenarios.
Space Complexity: The space complexity is O(x)



Summary of executions:
Singel Thread: 
INFO: Execution time with MAX_ROWS = 2: 31950 milliseconds
INFO: Execution time with MAX_ROWS = 5: 10211 milliseconds
INFO: Execution time with MAX_ROWS = 10: 5367 milliseconds
INFO: Execution time with MAX_ROWS = 20: 2650 milliseconds


Multi Threaded (4 Thread Core i7 computer) 
INFO: MAX_ROWS = 2: 33502 milliseconds
INFO: MAX_ROWS = 20: 2189 milliseconds
INFO: MAX_ROWS = 5: 10869 milliseconds
INFO: MAX_ROWS = 10: 4599 milliseconds
