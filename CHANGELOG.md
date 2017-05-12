
## 0.5.x (master)

### New features

  - Select a subset of trails with `tdb cli` using the `--uuids` flag.

  - Optimized filters that match all events or no events. These can be used to [create a (materialized) view over a subset of trails](http://traildb.io/docs/technical_overview/#whitelist-or-blacklist-trails-a-view-over-a-subset-of-trails).

  - `tdb merge` supports merging of TrailDBs with mismatching sets of fields. The result is a union of all fields in the source TrailDBs.

  - `TDB_OPT_CONS_NO_BIGRAMS` option for [tdb_cons_set_opt](http://traildb.io/docs/api/#tdb_cons_set_opt) to disable bigram-based size optimization. This option can sometimes greatly speed up TrailDB creation at the cost of increased filesize. The flag can also be passed to `tdb` CLI as `--no-bigrams`.

  - Trail-level options: [tdb_set_trail_opt](http://traildb.io/docs/api/#tdb_set_trail_opt). This is especially useful for creating granular views using `TDB_OPT_EVENT_FILTER`. See [Setting Options](http://traildb.io/docs/api/#setting-options).

  - Time-range term: [query events within a given time-range](http://traildb.io/docs/api/#tdb_event_filter_add_time_range). This simplifies time-series type analyses of trails. Also expanded the filter inspection API to add functions for [counting the number of terms in a clause](http://traildb.io/docs/api/#tdb_event_filter_num_terms), [inspecting the type of a term](http://traildb.io/docs/api/#tdb_event_filter_get_term_type), and [returning the start and end times of a time-range term](http://traildb.io/docs/api/#tdb_event_filter_get_time_range).

  - Multi-cursors: [join trails over multiple TrailDBs efficiently](http://traildb.io/docs/api/#join-trails-with-multi-cursors). This is a convenient way to stich together e.g. time-sharded TrailDBs or merge together user profiles stored under separate UUIDs.

  - Item index for `tdb` CLI. This can speed up `--filter` expressions that access infrequent items
    significantly.

  - Added [tdb_event_filter_num_clauses](http://traildb.io/docs/api/#tdb_event_filter_num_clauses)
    and [tdb_event_filter_get_item](http://traildb.io/docs/api/#tdb_event_filter_get_item) for reading
    items and clauses in an existing filter.

  - `TDB_OPT_EVENT_FILTER` option for `tdb_set_opt` which can be used to create
    [views](http://traildb.io/docs/technical_overview/#return-a-subset-of-events-with-event-filters)
    or [materialized views](http://traildb.io/docs/technical_overview/#create-traildb-extracts-materialized-views).

  - `--filter` flag for the `tdb` CLI: Define event filters on the
    command line for easy grepping of events. Also added `--verbose` flag
    for troubleshooting filters.

  - `tdb merge` for `tdb` CLI. This operation is used to merge multipled tdbs into a single tdb.

  - Added a `brew` package for OS X.

  - Added installation instructions for FreeBSD.

### Bugfixes

  - Make opening single-file tdbs thread-safe.

  - Fix handling of empty values in `tdb_cons_append`.

  - Fix handling of disk full situations in `tdb_cons_append`.

  - Fix semantics of how `TDB_OPT_EVENT_FILTER` filters are applied to cursors.
    Now the changes are applied at every call to `tdb_get_trail`, not at the
    creation of the cursor.

## 0.5 (2016-05-24)

Initial open-source release.
