TOP = $(shell cd ../.. && pwd)
include $(TOP)/Makefile.inc

CFLAGS = -Werror -Wall -g -fPIC \
	$(CMB_CFLAGS) $(UTIL_CFLAGS)
LDFLAGS = $(CMB_LIBS) $(UTIL_LIBS)

LIBZUTIL_SOURCES := zio.c cbuf.c forkzio.c kz.c

TEST_programs := zio-test flux-zio-test

all: libzutil.a

libzutil.a: $(LIBZUTIL_SOURCES:.c=.o)
	ar rcs $@ $^

tests: libzutil.a $(TEST_programs)

%-test : %-test.o
	$(CC) -o $@ $< $(CFLAGS) $(LDFLAGS) \
	 libzutil.a ../util/libutil.a -ljson -lczmq -lcmb
clean:
	rm -f *.a *.o $(TEST_programs)
