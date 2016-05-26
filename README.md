# TrailDB

<img src="http://traildb.io/images/traildb_logo_512.png">

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

	$ brew install judy libarchive pkg-config

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

    $ ./waf install --test_build
    $ ./test-driver.py

On FreeBSD, you may need to: `CFLAGS='-L/usr/local/lib -I/usr/local/include' ./test-driver.py`

or on Linux

    $ make distclean
    $ cd tests
    $ ./coverage.py

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/traildb/traildb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
