# TrailDB

<img src="http://traildb.io/images/tdb_logo@2x.png" style="width: 40%">

TrailDB is an efficient tool for storing and querying series of events.
This repository contains the core C library and the `tdb` command line tool.

Learn more at [traildb.io](http://traildb.io).

## Quick start

For detailed installation instructions, see [Getting Started guide](http://traildb.io/docs/getting_started/).

#### Install Dependencies

	$ apt-get install libarchive-dev libjudy-dev pkg-config

For RPM-based distros:

	$ yum install judy-devel libarchive-devel pkg-config

For OSX:

	$ brew install judy libarchive

#### Build TrailDB

    $ ./waf configure
    $ ./waf install

