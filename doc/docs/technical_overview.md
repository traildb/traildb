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
 - Maximum number of distinct values in a field: 2<sup>40</sup> - 2
 - Maximum number of fields: 16,382

# Internals

TrailDB uses a number of different compression methods to minimize
the amount of space used. In contrast to the usual approach of
compressing a file for storage, and decompressing it for processing,
TrailDB compresses the data when TrailDB is constructed but it never
decompresses all the data again. Only the parts that are actually
requested, using a lazy cursor, are decompressed on the fly. This makes
TrailDB cache-friendly and hence fast to query.

The data model of TrailDB enables efficient compression: Since
events are grouped by UUID, which typically corresponds a user,
a server, or other such logical entity, events within a trail
tend to be somewhat predictable - each logical entity tends
to behave in its own characteric way. We can leverage this by
only encoding every change in behavior, similar to [run-length
encoding](http://en.wikipedia.org/wiki/Run-length_encoding).

Another observation is that since in the TrailDB data
model events are always sorted by time, we can utilize
[delta-encoding](http://en.wikipedia.org/wiki/Delta_encoding) to
compress 64-bit timestamps that otherwise would end up consuming a
non-trivial amount of space.

After these baseline transformations, we can observe that the
distribution of values is typically very skewed: Some items are
way more frequent than others. We also analyze the distribution
of pairs of values (bigrams) for similar skewedness. Finally,
we employ both variable-length integers as well as [Huffman
coding](http://en.wikipedia.org/wiki/Huffman_coding) to efficiently
encode the transformed data.

The end result is often comparable to compressing the data using Zip.
The big benefit of TrailDB compared to Zip is that each trail can be
decoded individually efficiently and the output of decoding is kept in
an efficent integer-based format. By design, the TrailDB API encourages
the use of integer-based items instead of original strings for further
processing, making it easy to build high-performance applications on top
of TrailDB.
