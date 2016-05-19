# TrailDB

### Install

install locally with autotools:

    $ ./autogen.sh
    $ ./configure
    $ make
    $ sudo make install

install locally with waf (depends on python):

    $ ./waf configure
    $ ./waf install

### Dependencies

For Debian and derivatives:

	$ apt-get install libarchive-dev libjudy-dev

For RPM-based distros:

	$ yum install judy-devel libarchive-devel

For OSX:

	$ brew install judy libarchive


### Tests

The quickest way to run the test suite is
    $ ./waf configure
    $ ./waf install
    $ sh test-driver.sh

Read the `README.md` inside `tests/` subdirectory.
