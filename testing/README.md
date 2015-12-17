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
turned on and installs it to a temporary directory. The tests are run with full
coverage information. Returns 0 if all tests succeeded and -1 if one or more
tests failed.

How to write tests
------------------

Write self-contained .c files and place them somewhere inside `c-files`
subdirectory. `test.py` compiles each .c file individually and runs it.
Successful test should return 0 as exit code. Look at the existing tests to see
what's going on.

Every test is run with one argument, a temporary directory which will be
cleaned up after the test exits. You can create a TrailDB in that directory.


