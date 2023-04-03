# Mongo ObjectID vs ULID vs UUID as document ID performance

Comparing index performance on using [ULID](https://github.com/ulid/spec)s, UUIDs and ObjectIDs s document `_id` field.

As for the April 2023 there is a plenty of vague statements on the Internet about Mongo performance when using
ObjectID vs UUID as document identifiers. Most of them refer to a 
[2012 performance test](https://github.com/Restuta/mongo.Guid-vs-ObjectId-performance).

The main reason why UUID is less performant comparing to Mongo's native ObjectID is because ObjectID starts with a 
timestamp and thus is sortable, while non-time-ordered UUID versions such as UUIDv4 are not and have poor database 
index  locality. Meaning new values created in succession are not close to each other in the index and thus require 
inserts  to be performed at random locations. The negative performance effects of which on common structures used 
for this (B-tree and its variants) can be dramatic.

Although there is no standard published yet that provides specification for a sortable identifier, there is 
a [draft](https://github.com/uuid6/uuid6-ietf-draft/issues/122) for UUID v6, v7 and v8 which are expected to be sortable.

Due to the aforementioned issue, many widely distributed database applications and large application vendors have 
sought to solve the problem of creating a better time-based, sortable unique identifier for use as a database key. 
This has lead to numerous implementations over the past 10+ years solving the same problem in slightly different ways.

One of them is the [ULID](https://github.com/ulid/spec) - Universally Unique Lexicographically Sortable Identifier. 
It is a UUID implementation that solves many of the previous UUID versions and, as it is said in the title, it is 
sortable.

Here I provide the results of indexing performance comparison when ULID, UUID and ObjectID are used as object IDs.

```
> Environment info
- go version go1.20.1 linux/amd64
- Mongo version 6.0.5

> Running the test...

| ---------------------------------------------------------------------- | ---------- | ------------ | ------------ | ------------ | ------------ |
| Test case                                                              | ObjectId   | ULID         | UUID         | % diff ULID  | % diff UUID  |
| ---------------------------------------------------------------------- | ---------- | ------------ | ------------ | ------------ | ------------ |
| 1M inserts batched, batch size = 1k                                    | 3.491s     | 4.028s       | 5.371s       | -15.38%      | -53.85%      |
| 1M inserts batched, batch size = 5k                                    | 3.335s     | 3.647s       | 5.122s       | -9.35%       | -53.57%      |
| 1M inserts batched, batch size = 10k                                   | 3.323s     | 3.523s       | 5.163s       | -6.04%       | -55.40%      |
| 1M inserts                                                             | 2m2.426s   | 2m12.299s    | 2m14.906s    | -8.06%       | -10.19%      |
| 10M inserts batched, 10M documents already present, batch size = 10k   | 31.804s    | 34.335s      | 1m3.972s     | -7.96%       | -101.14%     |
| 10M inserts batched, 10M documents already present, batch size = 100k  | 31.804s    | 34.335s      | 1m3.972s     | -7.96%       | -101.14%     |
| Index size with 20M docs in bytes                                      | 192.0 MiB  | 266.4 MiB    | 756.2 MiB    | -38.78%      | -293.96%     |
| Get by ID from 20M docs, avg duration                                  | 154µs      | 150µs        | 167µs        | -1.96%       | -8.50%       |
| ---------------------------------------------------------------------- | ---------- | ------------ | ------------ | ------------ | ------------ |

Total execution time: 15m29.354s
```

To reproduce locally you will require:

- make
- docker
- golang

and run:

```bash
make run
```
