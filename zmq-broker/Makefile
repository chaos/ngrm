TOP = $(shell cd .. && pwd)
include $(TOP)/Makefile.inc

ZTOP =             $(shell pwd)
LIBDIR =          $(ZTOP)/lib
EXTRA_CFLAGS +=   -I$(ZTOP) -I$(ZTOP)/include $(UTIL_CFLAGS) -I$(ZTOP)/zutil
EXTRA_LDFLAGS +=  -L$(ZTOP)/lib $(ZTOP)/zutil/libzutil.a $(ZTOP)/util/libutil.a

SUBDIRS :=        util wreck zutil
SUBDIRS_POST :=   dlua test

CFLAGS = -Werror -Wall -g -fPIC $(EXTRA_CFLAGS) \
	-I/usr/include/czmq \
	-DPLUGIN_PATH=\"$(ZTOP)/plugins\" \
	-DEXEC_PATH=\"$(ZTOP)/libexec\" $(LUA_CFLAGS)
LDFLAGS = -Wl,-rpath,$(LIBDIR)

export CFLAGS LDFLAGS LUA_CFLAGS LUA_LIBS

LIBUTIL = util/libutil.a
LIBZUTIL = zutil/libzutil.a
LDADD_CLI = $(EXTRA_LDFLAGS) -ljson -lzmq -lczmq -lutil -lm -lmunge
LDADD_SRV = -pthread -rdynamic $(LDADD_CLI) -ldl

COMMON_OBJS = kvscli.o barriercli.o logcli.o cmbdcli.o mrpc.o handle.o \
	waitqueue.o security.o eventcli.o rankcli.o reduce.o
CMBD_OBJS = plugin.o $(COMMON_OBJS) cmbd.o

FLUX_OBJS = flux.o
LIBCMB_OBJS = apicli.o $(COMMON_OBJS)
LIBRDL_OBJS = rdl.o dlua/json-lua.o util/list.o

LIBCMB = $(LIBDIR)/libcmb.so.0
LIBRDL = $(LIBDIR)/librdl.so.0

PLUGINS = plugins/api.so plugins/hb.so plugins/kvs.so \
	plugins/barrier.so plugins/live.so plugins/mecho.so \
	plugins/log.so plugins/echo.so plugins/job.so \
	plugins/resrc.so plugins/rexec.so plugins/sched.so \
	plugins/event.so plugins/rank.so plugins/mon.so

SUBCMDS = libexec/kvs libexec/ping libexec/mecho \
	libexec/stats libexec/barrier libexec/snoop libexec/event \
	libexec/kvswatch libexec/kvsdir libexec/kvstorture \
	libexec/logger libexec/log libexec/info libexec/kvscopy \
	libexec/zio libexec/up libexec/host libexec/keygen libexec/mon \
	libexec/rmmod libexec/lsmod libexec/insmod libexec/modprobe \
	libexec/rdltool

BUILD = $(LIBCMB) $(LIBRDL) flux echo cmbd \
	$(PLUGINS) $(SUBCMDS)

all:
	@for f in $(SUBDIRS); do make -C $$f $@; done
	make $(BUILD)
	@for f in $(SUBDIRS_POST); do make -C $$f $@; done

plugins/%.so: %srv.c
	mkdir -p plugins
	$(CC) -shared $(CFLAGS) $(LDFLAGS) -o $@ $< $(UTIL_LIBS)
plugins/rexec.so: wreck/wrexecsrv.c
	mkdir -p plugins
	$(CC) -shared $(CFLAGS) $(LDFLAGS) -o $@ $< $(UTIL_LIBS)
plugins/%.so: wreck/%srv.c
	mkdir -p plugins
	$(CC) -shared $(CFLAGS) $(LDFLAGS) -o $@ $< $(UTIL_LIBS)

subdirs:
	@for f in $(SUBDIRS); do make -C $$f; done
subdirs_post:
	@for f in $(SUBDIRS_POST); do make -C $$f; done

cmbd: $(CMBD_OBJS)
	$(CC) -o $@ $(CMBD_OBJS) $(LDADD_SRV)

flux: $(FLUX_OBJS) $(LIBCMB)
	$(CC) $(LDFLAGS) -o $@ $(FLUX_OBJS) $(LDADD_CLI)

libexec/rdltool : flux-rdltool.o
	$(CC) $(LDFLAGS) -o $@ $< -L$(LIBDIR) -lrdl $(LDADD_CLI) $(LUA_LIBS)

libexec/%: flux-%.o
	mkdir -p $(shell dirname $@)
	$(CC) $(LDFLAGS) -o $@ $< -L$(LIBDIR) -lcmb $(LDADD_CLI)

$(LIBCMB): $(LIBCMB_OBJS) $(LIBUTIL)
	mkdir -p $(shell dirname $@)
	$(CC) -shared -Wl,--version-script=cmb_version.map \
		-o $@ $(LIBCMB_OBJS) $(LDADD_CLI)
	ln -sf $(shell basename $@) $(@:.0=)

$(LIBRDL): $(LIBRDL_OBJS) $(LIBUTIL)
	mkdir -p $(shell dirname $@)
	$(CC) -shared -Wl,--version-script=rdl_version.map \
		-o $@ $(LIBRDL_OBJS) $(LDADD_CLI) -ljson
	ln -sf $(shell basename $@) $(@:.0=)

$(LIBUTIL):
	@make -C util
$(LIBZUTIL):
	@make -C zutil

echo: echo.o
	$(CC) $(LDFLAGS) -o $@ $< -L$(LIBDIR) -lcmb $(LDADD_CLI)

clean:
	@for f in $(SUBDIRS); do make -C $$f $@; done
	rm -f *.o *.so $(BUILD)
	rm -rf libexec plugins $(LIBDIR)
	@for f in $(SUBDIRS_POST); do make -C $$f $@; done

# header dependencies
OBJS = $(CMBD_OBJS) $(FLUX_OBJS) $(LIBCMB_OBJS)
INCLUDES = $(wildcard *.h include/*.h)
$(OBJS): $(INCLUDES)
