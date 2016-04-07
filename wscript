# How to use WAF:
# wget https://waf.io/waf-1.8.20 -O waf
# chmod +x waf
# git commit waf   # this is supposed to be committed in SCM
# ./waf configure
# ./waf            # or ./waf-1.8.20 build
# ./waf install
# ./waf uninstall

APPNAME = "traildb"
VERSION = "0.1"

def options(opt):
    opt.load("compiler_c")

def configure(cnf):
    cnf.load("compiler_c")

    cnf.define("DSFMT_MEXP", 521)
    cnf.define("HAVE_SSE2", 1)
    cnf.env.append_value("CFLAGS", "-std=c99")
    cnf.env.append_value("CFLAGS", "-O3")
    cnf.env.append_value("CFLAGS", "-g")

    # Find libarchive through pkg-config. On OSX, there is a
    # very old pre-installed version which is not compatible,
    # so error out if major version is < 3
    cnf.check_cfg(package="libarchive",
        args=["libarchive >= 3.0.0", "--cflags", "--libs"],
        uselib_store="ARCHIVE")

    # Judy does not support pkg-config. Do a normal dependeny
    # check.
    cnf.check_cc(lib="Judy", uselib_store="JUDY")

def build(bld):
    tdbcflags = [
        "-Wextra",
        "-Wconversion",
        "-Wcast-qual",
        "-Wformat-security",
        "-Wmissing-declarations",
        "-Wmissing-prototypes",
        "-Wnested-externs",
        "-Wpointer-arith",
        "-Wshadow",
        "-Wstrict-prototypes",
    ]

    # Build traildb shared library
    bld.shlib(
        target         = "traildb",
        source         = bld.path.ant_glob("src/**/*.c"),
        cflags         = tdbcflags,
        uselib         = ["ARCHIVE", "JUDY"],
        vnum            = "0",  # .so versioning
    )

    # Build traildb static library
    bld.stlib(
        target         = "traildb",
        source         = bld.path.ant_glob("src/**/*.c"),
        cflags         = tdbcflags,
        uselib         = ["ARCHIVE", "JUDY"],
        install_path   = "${PREFIX}/lib",  # opt-in to have .a installed
    )

    # Build traildb_bench
    bld.program(
        target       = "traildb_bench",
        source       = "util/traildb_bench.c",
        includes     = "src",
        use          = "traildb",
        uselib       = ["ARCHIVE", "JUDY"],
    )

    # Build tdbcli
    bld.program(
        target       = "tdbcli",
        source       = bld.path.ant_glob("tdb/**/*.c"),
        includes     = "src",
        use          = "traildb",
        uselib       = ["ARCHIVE", "JUDY"],
    )

    # Mark header files that must be installed
    bld.install_files("${PREFIX}/include", [
        "src/traildb.h",
        "src/tdb_error.h",
        "src/tdb_types.h",
        "src/tdb_limits.h"
    ])
