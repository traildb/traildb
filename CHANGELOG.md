
## 0.5.x (master)

### New features

  - Added [tdb_event_filter_num_clauses](http://traildb.io/docs/api/#tdb_event_filter_num_clauses)
    and [tdb_event_filter_get_item](http://traildb.io/docs/api/#tdb_event_filter_get_item) for reading
    items and clauses in an existing filter.

  - `TDB_OPT_EVENT_FILTER` option for `tdb_set_opt` which can be used to create
    [views](http://traildb.io/docs/technical_overview/#return-a-subset-of-events-with-event-filters)
    or [materialized views](http://traildb.io/docs/technical_overview/#create-traildb-extracts-materialized-views).

  - `--filter` flag for the `tdb` CLI: Define event filters on the
    command line for easy grepping of events. Also added `--verbose` flag
    for troubleshooting filters.

  - Added installation instructions for FreeBSD.

### Bugfixes

  - Make opening single-file tdbs thread-safe

## 0.5 (2016-05-24)

Initial open-source release.
