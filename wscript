# How to use WAF:
# wget https://waf.io/waf-1.8.20 -O waf
# chmod +x waf
# git commit waf   # this is supposed to be committed in SCM
# ./waf configure
# ./waf            # or ./waf build
# ./waf install
# ./waf uninstall
import sys, os
import tempfile, shutil

APPNAME = "traildb"
VERSION = "0.6"

SKIP_TESTS = []
if sys.platform == "darwin":
    SKIP_TESTS += ["judy_128_map_test.c", "out_of_memory.c"]

errmsg_libarchive = "not found"
errmsg_judy = "not found"

if sys.platform == "darwin":
    errmsg_libarchive = "not found; install with 'brew install libarchive'"
    errmsg_judy = "not found; install with 'brew install traildb/judy/judy'"

def configure(cnf):
    cnf.load("compiler_c")

    cnf.define("DSFMT_MEXP", 521)
    cnf.define("HAVE_ARCHIVE_H", 1)
    cnf.env.append_value("CFLAGS", "-std=c99")
    cnf.env.append_value("CFLAGS", "-O3")
    cnf.env.append_value("CFLAGS", "-g")

    # Find libarchive through pkg-config. We need at least version 3.
    # 
    # On macOS, libarchive is pre-installed but is not pkg-config-findable.
    # Homebrew version is a keg-only package.
    #
    # We'll set the path so that we'll try to find that keg-package.
    if sys.platform == "darwin":
        cellar_dir = "/usr/local/Cellar/libarchive"
        best_candidate = None
        best_candidate_dir = None
        try:
            # Poor man's version checking, we'll look for version numbers inside
            # a path, turn them into tuples (so they'll compare correctly)
            # and look for highest match.
            for dirname in list(os.listdir(cellar_dir)):
                candidate = tuple(dirname.split('.'))
                if best_candidate is None or best_candidate < candidate:
                    best_candidate = candidate
                    best_candidate_dir = dirname
        except FileNotFoundError:
            pass
        except PermissionError:
            pass
        if best_candidate is not None:
            pkg_path = os.path.join(cellar_dir, best_candidate_dir, 'lib', 'pkgconfig')
            os.environ["PKG_CONFIG_PATH"] = pkg_path
        else:
            os.environ["PKG_CONFIG_PATH"] = "/usr/local/opt/libarchive/lib/pkgconfig"

    if "PKG_CONFIG_PATH" in os.environ:
        cnf.env.env = {'PKG_CONFIG_PATH': os.environ["PKG_CONFIG_PATH"]}

    cnf.check_cfg(package="libarchive",
        args=["libarchive >= 3.0.0", "--cflags", "--libs"],
        uselib_store='ARCHIVE', errmsg=errmsg_libarchive)

    # Judy does not support pkg-config. Do a normal dependeny
    # check.
    cnf.check_cc(lib="Judy", uselib_store="JUDY",
        errmsg=errmsg_judy)

    # Check if Judy contains a known bug; if so, fail the configuration
    # step and ask the user to upgrade.
    cnf.check_cc(msg="Checking Judy version", fragment=JUDY_TEST,
        uselib="JUDY", features="c cprogram test_exec",
        errmsg="Found a broken version of Judy. Install a newer version.")

def options(opt):
    opt.load("compiler_c")

def build(bld, test_build=False):
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
        "-Wstrict-prototypes"
    ]
    if bld.variant == "test":
        tdbcflags.extend([
            "-DEVENTS_ARENA_INCREMENT=100",
            "-fprofile-arcs",
            "-ftest-coverage",
            "--coverage",
            "-fPIC",
        ])
    else:
        tdbcflags.append("-fvisibility=hidden")

    bld.stlib(
        target         = "traildb",
        source         = bld.path.ant_glob("src/**/*.c"),
        cflags         = tdbcflags,
        uselib         = ["ARCHIVE", "JUDY"],
        install_path   = "${PREFIX}/lib",  # opt-in to have .a installed
    )

    if bld.variant == "test":
        bld.load("waf_unit_test")
        basetmp = tempfile.mkdtemp()
        os.environ["TDB_TMP_DIR"] = "."

        for test in bld.path.ant_glob("tests/c-tests/*.c"):
            testname = os.path.basename(test.abspath())
            if testname in SKIP_TESTS:
                continue
            tsk = bld.program(
                features    = "test",
                target      = os.path.splitext(testname)[0],
                source      = [test],
                includes    = "src",
                cflags      = ["-fprofile-arcs", "-ftest-coverage", "-fPIC", "--coverage"],
                ldflags     = ["-fprofile-arcs"],
                use         = ["traildb"],
                uselib      = ["ARCHIVE", "JUDY"],
            )
            tsk.ut_cwd = basetmp+"/"+testname
            os.mkdir(tsk.ut_cwd)

        from waflib.Tools import waf_unit_test
        bld.add_post_fun(waf_unit_test.summary)
        bld.add_post_fun(waf_unit_test.set_exit_code)
        bld.add_post_fun(lambda _: shutil.rmtree(basetmp))
        return

    bld.shlib(
        target         = "traildb",
        source         = bld.path.ant_glob("src/**/*.c"),
        cflags         = tdbcflags,
        uselib         = ["ARCHIVE", "JUDY"],
        vnum            = "0",  # .so versioning
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
        source       = bld.path.ant_glob("tdbcli/**/*.c") +
                       bld.path.ant_glob("src/xxhash/*.c"),
        includes     = "src",
        use          = "traildb",
        ldflags      = ["-pthread"],
        uselib       = ["JUDY"],
    )

    # Build libtdbindex.so
    bld.shlib(
        target       = "tdbindex",
        source       = ["tdbcli/tdb_index.c", "tdbcli/thread_util.c"] +
                       bld.path.ant_glob("src/xxhash/*.c"),
        includes     = "src",
        use          = "traildb",
        ldflags      = ["-pthread"],
        uselib       = ["JUDY"],
        vnum            = "0",  # .so versioning
    )

    if bld.variant == "test_cli":
        import waflib
        bld.add_post_fun(lambda b: b.cmd_and_log('python tests/tdbcli/test_tdbcli.py',
                                                 output=waflib.Context.BOTH,
                                                 env={'TDBCLI_ROOT': 'build/test_cli'}))
        return

    # Mark header files that must be installed
    bld.install_files("${PREFIX}/include", [
        "src/traildb.h",
        "src/tdb_error.h",
        "src/tdb_types.h",
        "src/tdb_limits.h"
    ])

    # Build pkgconfig metadata file.
    # We install into /usr/share/pkgconfig as that one seems to be used across all distributions.
    # Otherwise, we might want to have distro-specific paths hardcoded here.
    pc = bld(source='traildb.pc.in', target='%s-%s.pc' % (APPNAME, VERSION), install_path='${PREFIX}/share/pkgconfig')
    pc.env.prefix  = bld.env.PREFIX
    pc.env.version = VERSION


from waflib.Build import BuildContext
class test(BuildContext):
        cmd = 'test'
        variant = 'test'

from waflib.Build import BuildContext
class test_cli(BuildContext):
        cmd = 'test_cli'
        variant = 'test_cli'


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
