
CFLAGS = -fPIC -O3 -Wall -g
CINCL  = -Isrc -Ideps/discodb/src
CLIBS  = -Llib $(foreach L,discodb Judy cmph m,-l$(L))
CHDRS  = $(wildcard src/*.h)
CSRCS  = $(wildcard src/*.c src/dsfmt/dSFMT.c)
COBJS  = $(patsubst %.c,%.o,$(CSRCS))

PYTHON = python

.PHONY: all bins libs clean python
.PHONY: deps/discodb/src

all: libs bins

bins: bin/encode bin/index bin/extractd bin/mix

libs: lib/libdiscodb.a lib/libtraildb.a lib/libtraildb.so

clean:
	rm -f $(COBJS)
	rm -f bin/encode bin/index bin/mix
	rm -f lib/libtraildb.*

bin/extractd: src/bin/extractd/*.c $(COBJS)
	$(CC) $(CFLAGS) $(CINCL) -I $(<D) -o $@ $^ $(CLIBS)

bin/mix: src/bin/mix/*.c src/bin/mix/ops/*.c $(COBJS)
	$(CC) $(CFLAGS) $(CINCL) -I $(<D) -o $@ $^ $(CLIBS)

bin/%: src/bin/%.c $(COBJS)
	$(CC) $(CFLAGS) $(CINCL) -o $@ $^ $(CLIBS)

deps/discodb/src:
	make -C deps/discodb CFLAGS="$(CFLAGS)"

lib/libdiscodb.a: deps/discodb/src
	$(AR) -ruvs $@ $(wildcard deps/discodb/src/*.o)

lib/libtraildb.a: $(COBJS)
	$(AR) -ruvs $@ $^

lib/libtraildb.so: $(COBJS)
	$(CC) $(CFLAGS) $(CINCL) -shared -o $@ $^ $(CLIBS)

src/dsfmt/%.o: src/dsfmt/%.c
	$(CC) $(CFLAGS) $(CINCL) -DDSFMT_MEXP=521 -DHAVE_SSE2=1 -msse2 -c -o $@ $^

src/%.o: src/%.c $(CHDRS)
	$(CC) $(CFLAGS) $(CINCL) -DENABLE_COOKIE_INDEX -c -o $@ $<

src/bin/mix/%.c: src/bin/mix/ops.h

src/bin/mix/ops.h: src/bin/mix/generate-ops.sh Makefile
	$< > $@

python: CMD = build
python: lib/libtraildb.so
	(cd lib/python && $(PYTHON) setup.py $(CMD))
