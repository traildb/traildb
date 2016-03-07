# Overview


TrailDB is a storage engine for aggregating event data and querying it.

### Data Model


TrailDB data is a collection of trails. Each trail consists of multiple events.
Each event has a number of string fields, some of them may be empty, and an
integer timestamp.


#### Terms

**traildb**: a single traildb, stored on disk as a directory.

**trail**: a sequence of **events**, identified by an **uuid**.

**cookie**: each **trail** in a TrailDB has a unique 16-byte identifier called **uuid**.

**event**: single item in a **trail**. Has a timestamp and a set of named **fields**.

**field**: each **event** has a set of fields, values are strings. Each event in 
a traildb has the same set of fields.

### Performance considerations

`libtraildb` is heavily optimized towards analytical workloads; therefore reading TrailDBs is fast,
creating them is somewhat expensive, and you cannot modify them after creating (you have to create
a new TrailDB instead).
