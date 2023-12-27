Single Thread:
Time Complexity:  O(n log n), where n is the number of records in the file. 
Space Complexity: The space complexity is O(x), where x is the maximum number of records in memory, as we ensure not to exceed this limit during sorting and merging.

Multi Threaded: 
Time Complexity:  With perfect scaling, using p threads could reduce time complexity to approximately O(n log n / p) in best-case scenarios.
Space Complexity: The space complexity is O(x)



Summary of executions:

CSV sorted successfully.
Dec 27, 2023 8:55:12 PM com.example.CSVSorter main
INFO: Total execution time: 31924 milliseconds
Dec 27, 2023 8:55:12 PM com.example.CSVSorterTest testDifferentMaxRowsValues
INFO: Execution time with MAX_ROWS = 2: 31950 milliseconds
CSV sorted successfully.
Dec 27, 2023 8:55:22 PM com.example.CSVSorter main
INFO: Total execution time: 10209 milliseconds
Dec 27, 2023 8:55:22 PM com.example.CSVSorterTest testDifferentMaxRowsValues
INFO: Execution time with MAX_ROWS = 5: 10211 milliseconds
CSV sorted successfully.
Dec 27, 2023 8:55:27 PM com.example.CSVSorter main
INFO: Total execution time: 5366 milliseconds
Dec 27, 2023 8:55:27 PM com.example.CSVSorterTest testDifferentMaxRowsValues
INFO: Execution time with MAX_ROWS = 10: 5367 milliseconds
CSV sorted successfully.
Dec 27, 2023 8:55:30 PM com.example.CSVSorter main
INFO: Total execution time: 2649 milliseconds
Dec 27, 2023 8:55:30 PM com.example.CSVSorterTest testDifferentMaxRowsValues
INFO: Execution time with MAX_ROWS = 20: 2650 milliseconds

Process finished with exit code 0



INFO: ----- Summary of Execution Times -----
Dec 27, 2023 8:59:32 PM com.example.MultiThreadedCSVSorterTest lambda$logSummary$0
INFO: MAX_ROWS = 2: 33502 milliseconds
Dec 27, 2023 8:59:32 PM com.example.MultiThreadedCSVSorterTest lambda$logSummary$0
INFO: MAX_ROWS = 20: 2189 milliseconds
Dec 27, 2023 8:59:32 PM com.example.MultiThreadedCSVSorterTest lambda$logSummary$0
INFO: MAX_ROWS = 5: 10869 milliseconds
Dec 27, 2023 8:59:32 PM com.example.MultiThreadedCSVSorterTest lambda$logSummary$0
INFO: MAX_ROWS = 10: 4599 milliseconds
