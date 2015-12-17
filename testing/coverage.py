#!/usr/bin/env python

import distutils.spawn
import tempfile
import sys
import os

def has_coverage_tools():
    return bool(distutils.spawn.find_executable("lcov") and distutils.spawn.find_executable("gcov"))

def run_coverage_test():
    # Here's what happens:
    # 1. We create a temporary directory
    # 2. We run ./configure from this project but build objects and stuff to
    #    the temporary directory
    # 3. We set up library path (LD_LIBRARY_PATH) to the temporary directory
    #    and then run tests (so the tests will use the temporarily compiled
    #    libraries)
    # 4. This thing shows test.py results on standard output so use can
    #    see if tests succeeded.
    # 5. (Optional) coverage files are turned to html and copied to current
    #    directory

    temp_dir_path = tempfile.mkdtemp()
    old_cwd = os.getcwd()
    upper_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
    has_coverage = has_coverage_tools()

    try:
        if has_coverage:
            os.putenv("CFLAGS", "-I%s/src --coverage" % upper_path)
        else:
            os.putenv("CFLAGS", "-I%s/src" % upper_path)

        os.system("cd %s && %s --prefix %s && make install" % (temp_dir_path, os.path.join(upper_path, "configure"), temp_dir_path))
        ld_lib_path = os.getenv("LD_LIBRARY_PATH") or ""
        os.putenv("LD_LIBRARY_PATH", "%s:%s" % (os.path.join(temp_dir_path, "lib"), ld_lib_path))
        result = os.system("cd %s && ./support/test.py" % old_cwd)

        if has_coverage:
            os.system("cd %s && lcov --capture --directory . --output-file gcov.info" % temp_dir_path)
            os.system("cd %s && genhtml gcov.info --output-directory %s" % (temp_dir_path, os.path.join(old_cwd, "coverage-html")))
        else:
            print("I will not generate coverage information because 'lcov' and/or 'gcov' is missing.")
        return result
    finally:
        try:
            os.system("cd %s" % old_cwd)
            shutil.rmtree(temp_dir_path)
        except:
            pass

if __name__ == '__main__':
    sys.exit(run_coverage_test())

