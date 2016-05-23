#!/bin/sh
set -euf
OLDCWD=$(pwd)

echo "Run ./waf install --test_build before running this!"

# add source directory
CFLAGS="${CFLAGS-} -L ${OLDCWD}/build -I${OLDCWD}/src"
export CFLAGS

export LD_LIBRARY_PATH=${OLDCWD}/build

# move to test directory
cd ./tests
# run python wrapper
./support/test.py
