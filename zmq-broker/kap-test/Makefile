#
# Author: Dong H. Ahn
#
#
# Update: 2014-01-29 DHA Import
#


CC	   	:= mpigcc
INCLUDES	:= -I$(shell ap ./) -I$(shell ap ../include) -I$(shell ap ../util) 
LIBFLAGS        := -L$(shell ap ../lib) -L$(shell ap ../util) -Wl,-rpath=$(shell ap ../lib)
LIBS            := -lutil -lczmq -lcmb
CFLAGS 		:= -g -O3 -Wall
HEADERS 	:= kap_opts.h        \
	   	   kap_personality.h \
	    	   kap.h

all: kap document

kap: kap.o kap_opts.o kap_personality.o kap_roles.o 
	$(CC) $(CFLAGS) $^ -o $@ $(LIBFLAGS) $(LIBS)

kap_opts.o: kap_opts.c $(HEADERS)
	$(CC) $(INCLUDES) $(CFLAGS) $< -c -o $@

kap_personality.o: kap_personality.c $(HEADERS)
	$(CC) $(INCLUDES) $(CFLAGS) $< -c -o $@

kap_roles.o: kap_roles.c $(HEADERS)
	$(CC) $(INCLUDES) $(CFLAGS) $< -c -o $@

kap.o: kap.c $(HEADERS)
	$(CC) $(INCLUDES) $(CFLAGS) $< -c -o $@

document: doc/doxy_conf.txt
	cd doc; doxygen doxy_conf.txt

clean:
	rm -fr *.o *~ *breakpoint* kap doc/html doc/latex doc/man
