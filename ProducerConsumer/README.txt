Program Compilation INSTRUCTIONS
1) gcc --version => Apple clang version 15.0.0 (clang-1500.3.9.4)
2) mkdir build
3) cmake ..
4) make
5) executable file ProcessFeed

Program execution INSTRUCTIONS
1) create input folder in build folder and place all the files
2) move to build folder 
3) run ./ProcessFeed
4) output file MultiplexedFile.txt file will be created 


development
CONSTRAINTS:
1) We can't open and cache all the files at onces in memory

APPROACH:(Producer- Consumer APPROACH)
1) We will fetch all the file names to be process in the map(fileIndexMap) which will store the index of lines read so far from this time 
2) Will we create a Priority Queue which will store the all the Read line from different files based on timestamp 
3) We will read one unread line from all the files in fileIndexMap and push it into the priority Queue
4) if there is no unread line left in file we will remove it from the file map fileIndexMap.
5) while reading one unread line from all the files we will update the minUnreadTimeStamp which is the minimum timestamp of last unread line in all the exiting files in the map
6) After reading one line from all the files we can start poping out element from the priority queue until the element timestamp is greater than last unread line timestamp
7) After poping elements we can again start reading one line from all the exiting files in the map
8) Once all files are read we will enable terminate=true and it will pop out all the element from the priority queue and write it in the file 



