TOP =             $(shell pwd)/../..
LIBDIR =          $(TOP)/lib
EXTRA_CFLAGS +=   -I$(TOP)/include -I$(TOP)/util
EXTRA_LDFLAGS +=  -L$(TOP)/lib $(TOP)/util/libutil.a

ifeq ($(shell hostname),jimbo.chaos)
MPIMOD = mvapich2-gnu-shmem
else
MPIMOD = mvapich2-gnu-psm
endif

LDFLAGS = -Wl,-rpath,$(LIBDIR)
CFLAGS += $(EXTRA_CFLAGS)
LDADD = $(EXTRA_LDFLAGS) -ljson -lzmq -lczmq -lcrypto -lutil -luuid -lcmb -lftb

BUILD = ftb_alltoall ftb_eventhandle_example ftb_example_callback_unsubscribe \
	ftb_grouping ftb_ib_port_status_publisher ftb_multiplecomp \
	ftb_notify_logger ftb_pingpong ftb_polling_logger ftb_simple_publisher \
	ftb_simple_subscriber ftb_throw_delay_mpi ftb_watchdog

all: $(BUILD)

ftb_alltoall: ftb_alltoall.c
	(source /etc/profile.d/modules.sh;\
		module load $(MPIMOD);\
		mpicc $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDADD))
ftb_grouping: ftb_grouping.c
	(source /etc/profile.d/modules.sh;\
		module load $(MPIMOD);\
		mpicc $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDADD))
ftb_throw_delay_mpi: ftb_throw_delay_mpi.c
	(source /etc/profile.d/modules.sh;\
		module load $(MPIMOD);\
		mpicc $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDADD))

ftb_eventhandle_example: ftb_eventhandle_example.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDADD)
ftb_watchdog: ftb_watchdog.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDADD)
ftb_example_callback_unsubscribe: ftb_example_callback_unsubscribe.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDADD)
ftb_ib_port_status_publisher: ftb_ib_port_status_publisher.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDADD) -libverbs
ftb_multiplecomp: ftb_multiplecomp.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDADD)
ftb_notify_logger: ftb_notify_logger.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDADD)
ftb_pingpong: ftb_pingpong.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDADD)
ftb_polling_logger: ftb_polling_logger.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDADD)
ftb_simple_publisher: ftb_simple_publisher.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDADD)
ftb_simple_subscriber: ftb_simple_subscriber.o
	$(CC) $(LDFLAGS) -o $@ $< $(LDADD)

clean:
	rm -f *.o a.out
	rm -f $(BUILD)
