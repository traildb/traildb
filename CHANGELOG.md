
## 0.5.x (master)

### New features

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

## 0.5 (2016-05-24)

Initial open-source release.
