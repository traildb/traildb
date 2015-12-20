#!/usr/bin/env python

import distutils.spawn
import tempfile
import sys
import os

def has_coverage_tools():
    return bool(distutils.spawn.find_executable("lcov") and distutils.spawn.find_executable("gcov"))

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

    try:
        cflags = "%s -I%s/include/ -L%s/lib" % (os.getenv('CFLAGS', ''),
                                                temp_dir_path,
                                                temp_dir_path)

        if has_coverage and coverage:
            os.putenv("CFLAGS", cflags + " --coverage")
        else:
            os.putenv("CFLAGS", cflags)

        os.system("cd %s && %s --prefix %s && make install" % (temp_dir_path, os.path.join(upper_path, "configure"), temp_dir_path))
        ld_lib_path = os.getenv("LD_LIBRARY_PATH", '')
        os.putenv("LD_LIBRARY_PATH", "%s:%s" % (os.path.join(temp_dir_path, "lib"), ld_lib_path))
        result = os.system("cd %s && ./support/test.py" % script_path)

        if has_coverage and coverage:
            os.system("cd %s && lcov --capture --directory . --output-file gcov.info" % temp_dir_path)
            os.system("cd %s && genhtml gcov.info --output-directory %s" % (temp_dir_path, os.path.join(old_cwd, "coverage-html")))
            print("Generated coverage information to current working directory in coverage-html. If you don't want to generate coverage information, run with --no-coverage.")
        else:
            if coverage and not has_coverage:
                print("I will not generate coverage information because 'lcov' and/or 'gcov' is missing.")
        return result
    finally:
        try:
            os.system("cd %s" % old_cwd)
            shutil.rmtree(temp_dir_path)
        except:
            pass

if __name__ == '__main__':
    if '--no-coverage' in sys.argv:
        sys.exit(run_coverage_test(coverage=False))
    else:
        sys.exit(run_coverage_test(coverage=True))

