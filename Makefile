
CFLAGS = -fPIC -O3 -Wall
CINCL  = -I src -I deps/discodb/src
CLIBS  = $(foreach L,Judy cmph m,-l$(L))
CHDRS  = $(wildcard src/*.h)
CSRCS  = $(wildcard src/*.c src/dsfmt/dSFMT.c deps/discodb/src/*.c)
COBJS  = $(patsubst %.c,%.o,$(CSRCS))

PYTHON = python

.PHONY: all bins libs clean python

all: libs bins

bins: bin/encode bin/index bin/merge bin/mix

libs: lib/libtraildb.a lib/libtraildb.so

clean:
	rm -f $(COBJS)
	rm -f bin/encode bin/index bin/mix
	rm -f lib/libtraildb.*

bin/merge: src/bin/merge/*.c $(COBJS)
	$(CC) $(CFLAGS) $(CINCL) -I $(<D) -o $@ $^ $(CLIBS)

bin/mix: src/bin/mix/*.c src/bin/mix/ops/*.c $(COBJS)
	$(CC) $(CFLAGS) $(CINCL) -I $(<D) -o $@ $^ $(CLIBS)

bin/%: src/bin/%.c $(COBJS)
	$(CC) $(CFLAGS) $(CINCL) -o $@ $^ $(CLIBS)

deps/discodb/src/%.o:
	make -C deps/discodb CFLAGS="$(CFLAGS)"

deps/%.git:
	git submodule update --init $(@D)

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
