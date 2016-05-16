# Data Model

<img src="../images/datamodel.png" style="width: 60%">

traildb
:   - TrailDB is a collection of **trails**.

trail
:   - A **trail** is identified by a user-defined
      [128-bit UUID](http://en.wikipedia.org/wiki/UUID) as well as an
      automatically assigned trail ID.
    - A **trail** consists of a sequence of **events**, ordered by time.

event
:   - An **event** corresponds to an event in time, related to an UUID.
    - An **event** has a 64-bit integer timestamp.
    - An **event** consists of a pre-defined set of **fields**.

field
:   - Each TrailDB follows a schema that is a list of fields.
    - A **field** consists of a set of values.

In a relational database, **UUID** would be the primary key, **event** would be a row,
and **fields** would be the columns.

The combination of a field ID and a value of the field is represented
as an **item** in the [C API]. Items are encoded as 64-bit integers, so
they can be manipulated efficiently.

# Limits

 - Maximum number of trails: 2<sup>59</sup> - 1
 - Maximum number of events in a trail: 2<sup>50</sup> - 1
 - Maximum number of distrinct values in a field: 2<sup>40</sup> - 2
 - Maximum number of fields: 16,382

# Performance Best Practices

 - Integers (32/64 bit items)
 - Expensive Functions
 - Cons is Expensive

# Internals

 - Decompress only what you need (lazy, cursor)
 - Variable-width items
 - Dictionary Encoding
 - Edge Encoding
 - Entropy Coding
 - Bigram coding

