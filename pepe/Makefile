TOP = $(shell cd .. && pwd)
include $(TOP)/Makefile.inc

LIBS := 	$(PMGR_LIBS) $(LUA_LIBS)
CFLAGS :=       $(PMGR_CFLAGS) $(LUA_CFLAGS)

OBJECTS :=     list.o log_msg.o pepe-lua.o pepe-launcher.o optparse.o
TARGET :=      pepe-launcher


all:  $(TARGET)
$(TARGET): $(OBJECTS)
	$(CC) -o $@ $^ $(LIBS)

.c.o:
	$(CC) -ggdb -Wall $(CFLAGS) $(EXTRA_CFLAGS) -o $@ -fPIC -c $<

clean:
	rm -f $(TARGET) $(OBJECTS)
