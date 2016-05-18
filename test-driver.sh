#!/bin/sh
set -euf
OLDCWD=$(pwd)

# append to CFLAGS if defined
CFLAGS="${CFLAGS-} -I${OLDCWD}/src"; export CFLAGS

# move to test directory
cd ./tests
# run python wrapper
./support/test.py
