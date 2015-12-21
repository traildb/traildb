TrailDB tests
=============

This directory contains tests for TrailDB.

Directory structure
-------------------

    coverage.py    The file you want to invoke to run tests.
    c-tests/       Contains tests written in C language.
    support/       Support python files for testing.

Invoking
--------

    $ ./coverage.py

This command compiles TrailDB from parent directory with profiling and coverage
turned on and installs it to a temporary directory. If `gcov` and `lcov` can be
found in `PATH`, the tests are run with full coverage information and a
directory called `coverage-html` is created in the current working directory.
Returns 0 if all tests succeeded and -1 if one or more tests failed.

Test installed TrailDB
----------------------

    $ ./support/test.py

This just runs the tests and uses whatever TrailDB has been installed (or is in
`LD_LIBRARY_PATH`).

How to write tests
------------------

Write self-contained .c files and place them somewhere inside `c-files`
subdirectory. `test.py` compiles each .c file individually and runs it.
Successful test should return 0 as exit code. Look at the existing tests to see
what's going on.

Every test is run with one argument, a temporary directory which will be
cleaned up after the test exits. You can create a TrailDB in that directory.


