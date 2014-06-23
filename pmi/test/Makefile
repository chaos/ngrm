TOP = $(shell cd ../.. && pwd)
include $(TOP)/Makefile.inc

CFLAGS = $(PMI_CFLAGS)
LDFLAGS = $(PMI_LIBS) -ljson -lzmq -lczmq -lutil -lm -lpthread

BUILD = tpmikvs

all: $(BUILD)

tpmikvs: tpmikvs.o
	$(CC) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(BUILD) *.o
