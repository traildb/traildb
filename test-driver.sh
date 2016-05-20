#!/bin/sh
set -euf
OLDCWD=$(pwd)

# force small allocations
CFLAGS="${CFLAGS-} -DEVENTS_ARENA_INCREMENT=100"
# add source directory
CFLAGS="${CFLAGS-} -I${OLDCWD}/src"
export CFLAGS

# move to test directory
cd ./tests
# run python wrapper
./support/test.py
