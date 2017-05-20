[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/traildb/traildb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Travis CI](https://travis-ci.org/traildb/traildb.svg?branch=master)](https://travis-ci.org/traildb/traildb)
[![Coverage Status](https://coveralls.io/repos/github/traildb/traildb/badge.svg?branch=master)](https://coveralls.io/github/traildb/traildb?branch=master)

# TrailDB

<img src="http://traildb.io/images/traildb_logo_512.png">

TrailDB is an efficient tool for storing and querying series of events.
This repository contains the core C library and the `tdb` command line tool.

Learn more at [traildb.io](http://traildb.io).

## Quick start

For detailed installation instructions, see [Getting Started guide](http://traildb.io/docs/getting_started/).

### Installing binaries

On OSX, TrailDB is available through homebrew:

    $ brew install traildb

Linux binaries are not available yet.

### Compiling and installing from source

#### Install Dependencies

	$ apt-get install libarchive-dev libjudy-dev pkg-config

For RPM-based distros:

	$ yum install Judy-devel libarchive-devel pkg-config

For OSX:

	$ brew install traildb/judy/judy libarchive pkg-config

For FreeBSD:

    $ sudo pkg install python libarchive Judy pkgconf gcc


Note that your systems package manager may have too old of [libjudy](https://sourceforge.net/projects/judy/).
You may also require a [patch](https://sourceforge.net/p/judy/patches/5/) if you are using gcc 4.9.

#### Build TrailDB

    $ ./waf configure
    $ ./waf install

Alternatively you may use autotools

    $ ./autogen.sh
    $ ./configure
    $ make
    $ make install

#### Run Tests

    $ ./waf test
