
## 0.5.x (master)

### New features

  - `TDB_OPT_EVENT_FILTER` option for `tdb_set_opt` which can be used to create
    [views](http://traildb.io/docs/technical_overview/#return-a-subset-of-events-with-event-filters)
    or [materialized views](http://traildb.io/docs/technical_overview/#create-traildb-extracts-materialized-views).

  - `--filter` flag for `tdb` CLI: Define event filters on the command
    line for easy grepping of events. Also added `--verbose` flag for
    troubleshooting filters.

  - Updated installation instructions for FreeBSD.

## 0.5 (2016-05-24)

Initial open-source release.
