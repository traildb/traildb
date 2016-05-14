# Data Model


TrailDB data is a collection of trails. Each trail consists of multiple events.
Each event has a number of string fields, some of them may be empty, and an
integer timestamp.


traildb
:	A single traildb, stored on disk as a directory.

trail
:	A sequence of events, identified by an uuid.

uuid
:	Each trail in a TrailDB has a unique 16-byte identifier called uuid.

trail id
:	Each trail in a TrailDB is also automatically assigned a numeric sequential id, unique within that traildb.

event
: 	A single item in a trail. Has a timestamp and a set of named fields.

field
:   Each event has a set of fields, values are strings. Each event in a traildb has the same set of fields.

# Performance Best Practices

Integers
Expensive Functions

# Limits

# Internals

Compression
