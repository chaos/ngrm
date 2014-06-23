TOP = $(shell cd ../.. && pwd)
include $(TOP)/Makefile.inc

CFLAGS = -Werror -Wall -g -fPIC $(UTIL_CFLAGS)

LIBUTIL_SOURCES := log.c hostlist.c util.c zmsg.c hljson.c lsdfatal.c \
		   optparse.c list.c tstat.c

all: libutil.a

libutil.a: $(LIBUTIL_SOURCES:.c=.o)
	ar rcs $@ $^

clean:
	rm -f *.a *.o
