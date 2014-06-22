LUA_PKG = $(shell pkg-config --exists lua && echo lua || echo lua5.1)
LUA_CFLAGS = $(shell pkg-config --silence-errors --cflags $(LUA_PKG))
LUA_LIBS = $(shell pkg-config --silence-errors --libs $(LUA_PKG))

PMGR_DIR = $(TOP)/foreign/pmgr_collective/pmgr_collective-1.0_7
PMGR_CFLAGS = -I$(PMGR_DIR)/include
PMGR_LIBS = $(PMGR_DIR)/lib/libpmgr_collective.a
