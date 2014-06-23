TOP = $(shell cd ../.. && pwd)
include $(TOP)/Makefile.inc

CFLAGS = $(PMI_CFLAGS)
LDFLAGS = $(PMI_LIBS) 

all: tpmikvs

tpmikvs: tpmikvs.o
	$(CC) -o $@ $< $(LDFLAGS)

clean:
	rm -f tpmikvs *.o
