# DBShark

DBShark is an embedded key-value B+tree-based database inspired by [boltdb](https://github.com/boltdb/bolt).

> [!WARNING]
> This project is a work-in-progress.

Some of key features are:
- Embedded
- Key-value store
- Range query
- Serializable isolation level
- Crash-safe
- Transactions are atomic
- Allow rollback
- Concurrent read transaction
- Write transactions are exclusive

Limitation:
- Currently, it's only working for linux
- No delete operation yet
- Only allow single instance

# Design

So, dbshark is a key-value store database I built using btree. 
Nothing fancy here - it supports the usual stuff you'd expect: you can have one write transaction or multiple read transactions running at the same time (yeah, it's serializable). 
You can do inserts, lookups, and range queries, pretty much like you'd use a `BTreeMap` in Rust. 
I made it durable by using WAL and taking snapshots every now and then.
Really, there's nothing new about the design - I pretty much followed what they teach in the CMU database series.

## B+Tree

Let me tell you how the btree part works. 
The whole database is basically made up of buckets. 
Each bucket is a btree, and there's one root btree that keeps track of all the other buckets. 
Each page in the database is about 4KB (you can change this if you want), and every node in the btree is just a page in the main database file.

The keys and values are just bytes - nothing special about them. 
We sort the keys by comparing the bytes one by one. 
Sometimes you might have a key and value that's too big to fit in one page - when that happens, we just put the content in what we call an overflow page and keep a reference to it in the btree node.

The database itself is really just a single btree where the key is the bucket name and the value is that bucket's root page ID. 
Each bucket works like any other key-value store - bytes in, bytes out.

To keep things efficient, we make sure each page can fit at least 4 items. 
The nodes in the btree is either one of two types: interior nodes and leaf nodes. 
Interior nodes are like signposts - they store keys and pointers to other nodes below them. 
Leaf nodes are where the actual data lives.

## Using Raw Bytes Key-Value Store

Here's the thing about user data - it's usually not just bytes, right? Sometimes you've got tables, graphs, or whatever else. 
But most of the time, we can turn this stuff into bytes and keep things ordered nicely.

If you've used something like SQLite, Postgres, or MySQL, you're used to having tables with columns. 
Well, we can do the same thing with key-value store. 
Let's say you've got a table in MySQL - that's just like having a bucket in dbshark. 
Need to store an unsigned integer? Just turn it into 4 or 8 bytes using big-endian encoding. 
Got a signed integer? No problem - we can shift it so instead of going from `-N` to `M`, it goes from `0` to `N+M` and encode it using big-endian encoding.
Strings? Just add zero at the end.
For tuples, just stick all the fields together.

Want an index? Easy - make another bucket where the key is your index key and the value is the primary key from your original table.
Need auto-increment? Just use another bucket to keep track of the last ID you used.

## Buffer Pool

Let's talk about how we handle pages in the database. 
There's this thing called the buffer pool that manages everything.
Most of the time, when you're working with the database, you're not actually reading from the file - you're using cached pages in the buffer pool.

Yeah, if the database crashes, anything in the buffer pool that hasn't been saved to disk is gone. 
But don't worry - we've got WAL that keeps track of everything we did, so we can get back to where we were before the crash.

The buffer pool can only hold so much stuff. 
When it gets full and needs to load something new, it has to kick out an old page.
When that happens, if the page being kicked out has changes, we save it to disk first.
Every so often, dbshark will save everything in the buffer pool to disk - this makes recovery faster if something goes wrong.

## Durability & Recovery

Here's how we make sure we don't lose any data. 
We use WAL (write ahead log) and do checkpoints every now and then. 
Every time something changes in the database, we write it down in the WAL file. 
We don't write each change individually - we batch them together to make it faster. 
When someone commits their transaction, we make sure all the WAL entries are safely on disk using `fsync`. 
(By the way, fsync doesn't always guarantee your stuff is really on disk - it depends on your system. But that's not something DBShark worries about.)

If the database crashes and comes back up, we just replay all those WAL entries and we're back in business.
But replaying everything from the beginning of time would take forever, so we do checkpoints.
During a checkpoint, we save everything in the buffer pool to disk.
This way, when we need to recover, we only need to replay the WAL since the last checkpoint.
We also roll the WAL file every so often so it doesn't get too big.

When we're saving a page from the buffer pool to disk, we actually write it to this thing called a double buffer first. 
Only after we're sure that worked, we write it to the main database file.
Why? Because sometimes writes can fail halfway through and mess things up.
If writing to the double buffer fails, no big deal - the main database is still fine. 
If writing to the main file fails, we can just copy the double buffer file over during recovery.
We use CRC to know whether the writes failed or not.

