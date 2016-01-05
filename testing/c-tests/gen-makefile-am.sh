#!/bin/bash

echo -n bin_PROGRAMS=
for test in *.c; do
    echo -n `basename $test .c` ''
done
echo

gen_cflags='-g3 --std=c99 -I${abs_top_srcdir}/src -I${includedir}'
gen_ldflags='-ltraildb -lJudy -L${libdir}'

for test in *.c; do
    echo `basename $test .c`_CFLAGS = ${gen_cflags}
    echo `basename $test .c`_LDFLAGS = ${gen_ldflags}
done

exit 0
