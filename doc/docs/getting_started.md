
TrailDB depends on two external libraries:

 - [Judy arrays](http://judy.sourceforge.net)
 - [Libarchive](http://www.libarchive.org)

You can install these libraries using a package manager as described below.

First, clone the latest version of TrailDB from GitHub:
```sh
git clone https://github.com/traildb/traildb
```

# Install on Linux

Here we assume you are installing TrailDB on Ubuntu / Debian. It is easy
to adapt the steps for other distributions.

Install the dependencies:
```sh
apt-get install libjudy-dev libarchive-dev pkg-config
```

Build TrailDB using `waf`
```sh
./waf configure
./waf build
```

`waf` writes the output to the `build` directory. You can install the
library and the `tdb` command line tool to a system-wide directory with
```sh
./waf install
```

That's all. See below for instructions for testing the installation.

Note that some old versions of Ubuntu and Debian include a version of
Judy that is broken (1.0.5-1 or older). The build will fail if a broken
version is found. You can install a newer version of Judy manually if
your system is affected.

Alternatively, TrailDB provides an autotools-based build system which
can be run as follows:
```sh
./autogen.sh
./configure
make
make install
```

# Install on OS X

### Using Homebrew

TrailDB package is available in Homebrew:

```sh
brew install traildb
```

### Building from source

Install the dependencies:

```sh
brew install judy libarchive pkg-config
```

Build TrailDB using `waf`
```sh
./waf configure
./waf build
```

`waf` writes the output to the `build` directory. You can install the
library and the `tdb` command line tool to a system-wide directory with
```sh
./waf install
```

# Test that it works

There is a small test file, `test.tdb`, included in the root of
the TrailDB repository. Once the `tdb` command line tool is built and
installed properly, you should be able to run

```sh
tdb dump -i test
```
at the root of the repository. You should see two events, `hello world`
and `it works!`, if everything works ok.

# Install Python bindings

The following [tutorial](tutorial) includes examples in C and Python. If
you want to use Python, you need to install the Python binding:

```sh
git clone https://github.com/traildb/traildb-python
```

Install the Python package with

```sh
python setup.py install
```

For other language bindings, follow the instructions in their README files.
