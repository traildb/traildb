import sys
import traildb

for cookie, trail in traildb.TrailDB(*(sys.argv[1:] or ['a.tdb'])).crumbs():
    print cookie, list(trail)
