Assumptions

1. When we write a file passed its size, it's impossible that the file size metadata is updated but
   their content is not written.
2. Fsync works.
3. When we write N bytes to a file, it's guaranteed that the write happen sequentially byte by byte.
