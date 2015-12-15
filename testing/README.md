TrailDB tests
=============

This directory contains tests for TrailDB.

Invoking
--------

    $ ./test.py

The above command would run the tests against system-installed TrailDB.

How to write tests
------------------

Write self-contained .c files and place them somewhere inside `c-files`
subdirectory. `test.py` compiles each .c file individually and runs it.
Successful test should return 0 as exit code. Look at the existing tests to see
what's going on.

Every test is run with one argument, a temporary directory which will be
cleaned up after the test exits. You can create a TrailDB in that directory.


