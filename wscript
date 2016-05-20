# How to use WAF:
# wget https://waf.io/waf-1.8.20 -O waf
# chmod +x waf
# git commit waf   # this is supposed to be committed in SCM
# ./waf configure
# ./waf            # or ./waf build
# ./waf install
# ./waf uninstall
import sys, os

APPNAME = "traildb"
VERSION = "0.1"

def options(opt):
    opt.load("compiler_c")

errmsg_libarchive = "not found"
errmsg_judy = "not found"

if sys.platform == "darwin":
    errmsg_libarchive = "not found; install with 'brew install libarchive'"
    errmsg_judy = "not found; install with 'brew install judy && brew link judy'"

def configure(cnf):
    cnf.load("compiler_c")

    cnf.define("DSFMT_MEXP", 521)
    cnf.define("HAVE_ARCHIVE_H", 1)
    cnf.env.append_value("CFLAGS", "-std=c99")
    cnf.env.append_value("CFLAGS", "-O3")
    cnf.env.append_value("CFLAGS", "-g")

    # Find libarchive through pkg-config. We need at least version 3.
    # On OSX, there is a very old pre-installed version which is not
    # compatible, so and brew never overwrites standard libraries, so
    # we need to explicitly tweak the PKG_CONFIG_PATH.
    if sys.platform == "darwin":
        os.environ["PKG_CONFIG_PATH"] = "/usr/local/opt/libarchive/lib/pkgconfig"
    cnf.check_cfg(package="libarchive",
        args=["libarchive >= 3.0.0", "--cflags", "--libs"],
        uselib_store="ARCHIVE", errmsg=errmsg_libarchive)

    # Judy does not support pkg-config. Do a normal dependeny
    # check.
    cnf.check_cc(lib="Judy", uselib_store="JUDY",
        errmsg=errmsg_judy)

    # Check if Judy contains a known bug; if so, fail the configuration
    # step and ask the user to upgrade.
    cnf.check_cc(msg="Checking Judy version", fragment=JUDY_TEST,
        uselib="JUDY", features="c cprogram test_exec",
        errmsg="Found a broken version of Judy. Install a newer version.")

def build(bld):
    tdbcflags = [
        "-Wextra",
        "-Wconversion",
        "-Wcast-qual",
        "-Wformat-security",
        "-Wformat",
        "-Wmissing-declarations",
        "-Wmissing-prototypes",
        "-Wnested-externs",
        "-Wpointer-arith",
        "-Wshadow",
        "-Wstrict-prototypes",
    ]


    bld.shlib(
        target         = "traildb",
        source         = bld.path.ant_glob("src/**/*.c"),
        cflags         = tdbcflags,
        uselib         = ["ARCHIVE", "JUDY"],
        vnum            = "0",  # .so versioning
    )

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
        target       = "tdb",
        source       = bld.path.ant_glob("tdbcli/**/*.c"),
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

    # Build pkgconfig metadata file
    pc = bld(source='traildb.pc.in', target='%s-%s.pc' % (APPNAME, VERSION), install_path='${PREFIX}/lib/pkgconfig')
    pc.env.prefix  = bld.env.PREFIX
    pc.env.version = VERSION


JUDY_TEST="""
#include <Judy.h>
#include <assert.h>
static const Word_t VALUES[] = {1,2,128,6492160,6570752,6741504,6781696,7320576,7447040,7618560,7631104,7849728,7883264,7939584,8057344,8201216,8884992,9279232,9296896,9538048,9664768,10105344,10193408,10336000,10800896,11067648,11117824,11325696,11423232,11468800,11548160,11570176,11682816,12011520,12019712,12033024,12135168,12503808,12576512,12594432,12673536,12752128,12891648,13351424,13618176,13730560,13813248,13890816,14390528,14923520,14957568,15025152,15035392,15282432,15331072,15340800,15500032,15563520,16437504,16505856,16629248,16652544,16931584,17140480,17163008,17184256,17275136,17348864,17383168,17459712,17489152,17516032,17663488,18044416,18263552,18349824,18487040,18508544,18595584,18685440,18772736,18914048,19010304,19168256,19441920,19479040,19567872,19621632,19724544,19802624,19834368,19962368,19990784,20011520,20069376,20165376,20189952,20287232,20352256,20365568,20383488,20433664,20484096,20513280,20570112,20688640,20843264,20901376,20966912,21228544,21248512,21345024,21348608,21394944,21451776,21504256,21528320,21657856,21669888,21763072,21777408,21795584,21803264,21838080,21845760,21849600,22038528,22051328,22112000,22340864,22428928,22445056,22453504,22462464,22529024,22736384,22797056,22964224,23027200,23195904,23205888,23220224,23249664,23289344,23588096,23609088,23693568,23781888};

int main() {
    Pvoid_t judy = NULL;
    int i, tst;
    Word_t tmp;
    Word_t num = sizeof(VALUES) / sizeof(VALUES[0]);
    for (i = 0; i < num; i++){
        J1S(tst, judy, VALUES[i]);
        assert(tst == 1);
    }

    J1C(tmp, judy, 0, -1);
    assert(tmp == num);

    for (i = 0; i < num; i++){
        J1U(tst, judy, VALUES[i]);
        assert(tst == 1);
    }
    return 0;
}
"""
