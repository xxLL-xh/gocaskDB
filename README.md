# gocaskDB
A KV database with high availability and high performance, based on the Bitcask paper.

# Abstract
The rapid development of information technology has brought our society into an era of information explosion, where efficient data read-and-write methods have become critical performance bottlenecks for various applications. Traditional relational database management systems (RDBS) have been the cornerstone of data processing for decades. However, their weaknesses in scalability and complexity lead to research interest in NoSQL databases, especially key-value (KV) storage databases.

This project begins with a review of mainstream KV database structures, comparing and analysing their advantages and disadvantages. Then, inspired by the Bitcask paper, this project designed and developed a KV database engine called gocaskDB. gocaskDB uses a Write-Ahead Log (WAL) mechanism to ensure data persistence and maintains a KeyDir in memory that maps each key in gocaskDB to the position of its associated log record on disk. This structure allows gocaskDB requires at most one disk I/O for both read and write operations.

The project implemented basic operations mentioned in the Bitcask paper, such as Put, Get, Delete, and Merge using Go language. It also improved gocaskDB by providing various KeyDir indexing structure options, accelerating startup process, enhancing merge operations, optimising persistence strategies, implementing atomic batch writes, and supporting HTTP and Redis Serialization Protocols.

Finally, extensive tests of gocaskDB were conducted, including unit tests, functional tests, and benchmark test. The test results show that all gocaskDB's functions work correctly and that the optimisations made to gocaskDB actually result in performance improvements. Benchmark test against popular KV databases such as GoLevelDB and boltDB shows that gocaskDB can deliver performance comparable to industrial databases under various workloads. 


