TOP = $(shell cd ../.. && pwd)
include $(TOP)/Makefile.inc


CFLAGS = $(CMB_CFLAGS) $(UTIL_CFLAGS)
LDFLAGS = $(CMB_LIBS) $(UTIL_LIBS) -ljson -lzmq -lczmq -lutil -lm -lpthread

BUILD = tenc tkvswatch tcommit tasyncsock tmunge

all: $(BUILD)

# for screen3
check1:
	export FLUX_API_PATH=/tmp/rank.1; make check
check2:
	export FLUX_API_PATH=/tmp/rank.2; make check
check3: check check1 check2

check:
	@for tst in $${TESTS:-t[0-9][0-9]}; do \
		./$$tst; \
		if [ $$? -eq 0 ]; then \
		  echo PASS: $$tst; \
		else \
		  echo FAIL: $$tst; \
		  exit 1; \
		fi \
	done

tenc: tenc.o
	$(CC) -o $@ $< $(LDFLAGS)

tkvswatch: tkvswatch.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDFLAGS)

tcommit: tcommit.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDFLAGS)

tasyncsock: tasyncsock.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDFLAGS)

tmunge: tmunge.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f *.o a.out
	rm -f $(BUILD)
