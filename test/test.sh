#!/bin/bash

outd=${1:-"test.tdb"}
fifo=${2:-"fifo"}

mkfifo $fifo

function cleanup () {
    rm $fifo
}

trap cleanup EXIT

tail -f > $fifo &
tpid=$!
bin/encode 'x y z' $outd < $fifo &
epid=$!

for k in {1..100}; do
    echo "12345678123456781234567812345678 1412896319 $k" > $fifo
done

kill $tpid
wait $epid
