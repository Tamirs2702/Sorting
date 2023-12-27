Single Thread:
Time Complexity:  O(n log n), where n is the number of records in the file. 
Space Complexity: The space complexity is O(x), where x is the maximum number of records in memory, as we ensure not to exceed this limit during sorting and merging.
Sorting Phase: O((n/x) * x log x) = O(n log x)
Merging Phase: O(n log k)

Multi Threaded: 
Time Complexity:  With perfect scaling, using p threads could reduce time complexity to approximately O(n log n / p) in best-case scenarios.
Space Complexity: The space complexity is O(x)

Full Execution Summary in result.txt 

