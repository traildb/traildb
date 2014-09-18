TrailDB is a database aggregating the past history of a cookie and letting it
you query it.

### Internals
Each dimension value is encoded into a 32 bits unsigned int.
The first byte encodes the dimension id. The 3 remaining bytes encode the
value.
