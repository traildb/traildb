import sys
import traildb

for cookie, trail in traildb.TrailDB(sys.argv[1]).crumbs():
    print cookie, list(trail)
