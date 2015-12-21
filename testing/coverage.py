#!/usr/bin/env python

import distutils.spawn
import tempfile
import sys
import os

class CommandFailed(Exception):
    pass

def has_coverage_tools():
    return bool(distutils.spawn.find_executable("lcov") and distutils.spawn.find_executable("gcov"))

def expect_zero_exit_code(*args, **kwargs):
    result = os.system(*args, **kwargs)
    if result != 0:
        sys.stderr.write("Command %s failed.\n" % str(*args))
        raise CommandFailed()
    return 0

def autoreconf_if_need_to(upper_path):
    # If there is configure script, we should be okay
    if os.path.isfile(os.path.join(upper_path, "configure")):
        return

    # ...but if not then we have to run autoreconf
    print("You don't have 'configure' script in source directory.")
    print("Running autoreconf -i for you...")

    old_cwd = os.getcwd()
    try:
        os.chdir(upper_path)
        expect_zero_exit_code("autoreconf -i")
    finally:
        os.chdir(old_cwd)

def run_coverage_test(coverage):
    # Here's what happens:
    # 1. We create a temporary directory
    # 2. We run ./configure from this project but build objects and stuff to
    #    the temporary directory
    # 3. We set up library path (LD_LIBRARY_PATH) to the temporary directory
    #    and then run tests (so the tests will use the temporarily compiled
    #    libraries)
    # 4. When we invoke `test.py`, results go to standard output so user can
    #    see if tests succeeded.
    # 5. (Optional) coverage files are turned to html and copied to current
    #    directory.

    temp_dir_path = tempfile.mkdtemp()
    script_path = os.path.dirname(os.path.realpath(__file__))
    old_cwd = os.getcwd()
    upper_path = os.path.abspath(os.path.join(script_path, ".."))
    has_coverage = has_coverage_tools()

    autoreconf_if_need_to(upper_path)

    try:
        cflags = "%s -I%s/src/ -L%s/.libs" % (os.getenv('CFLAGS', ''),
                                              upper_path,
                                              temp_dir_path)

        if has_coverage and coverage:
            os.putenv("CFLAGS", cflags + " --coverage")
        else:
            os.putenv("CFLAGS", cflags)

        expect_zero_exit_code("cd %s && %s --prefix %s && make install" % (temp_dir_path, os.path.join(upper_path, "configure"), temp_dir_path))
        ld_lib_path = os.getenv("LD_LIBRARY_PATH", '')
        os.putenv("LD_LIBRARY_PATH", "%s:%s" % (os.path.join(temp_dir_path, "lib"), ld_lib_path))
        result = os.system("cd %s && ./support/test.py" % script_path)

        if has_coverage and coverage:
            expect_zero_exit_code("cd %s && lcov --capture --directory . --output-file gcov.info" % temp_dir_path)
            expect_zero_exit_code("cd %s && genhtml gcov.info --output-directory %s" % (temp_dir_path, os.path.join(old_cwd, "coverage-html")))
            print("Generated coverage information to current working directory in coverage-html. If you don't want to generate coverage information, run with --no-coverage.")
        else:
            if coverage and not has_coverage:
                print("I will not generate coverage information because 'lcov' and/or 'gcov' is missing.")
        return result
    finally:
        try:
            os.chdir(old_cwd)
            shutil.rmtree(temp_dir_path)
        except:
            pass

if __name__ == '__main__':
    if '--no-coverage' in sys.argv:
        sys.exit(run_coverage_test(coverage=False))
    else:
        sys.exit(run_coverage_test(coverage=True))

