LUA_PKG = $(shell pkg-config --exists lua && echo lua || echo lua5.1)
LUA_CFLAGS = $(shell pkg-config --silence-errors --cflags $(LUA_PKG))
LUA_LIBS = $(shell pkg-config --silence-errors --libs $(LUA_PKG))

PMGR_DIR = $(TOP)/foreign/pmgr_collective/pmgr_collective-1.0_7
PMGR_CFLAGS = -I$(PMGR_DIR)/include
PMGR_LIBS = $(PMGR_DIR)/lib/libpmgr_collective.a

UTIL_DIR = $(TOP)/zmq-broker/util
UTIL_CFLAGS = -I$(UTIL_DIR) -DWANT_RECKLESS_HOSTRANGE_EXPANSION \
	-DWITH_LSD_NOMEM_ERROR_FUNC -DWITH_LSD_FATAL_ERROR_FUNC
UTIL_LIBS = $(UTIL_DIR)/libutil.a

ZUTIL_DIR = $(TOP)/zmq-broker/zutil
ZUTIL_CFLAGS = -I$(ZUTIL_DIR) 
ZUTIL_LIBS = $(ZUTIL_DIR)/libzutil.a

CMB_DIR = $(TOP)/zmq-broker
CMB_CFLAGS = -I$(CMB_DIR) -I$(CMB_DIR)/include
CMB_LIBS = -Wl,-rpath,$(CMB_DIR)/lib -L$(CMB_DIR)/lib -lcmb

PMI_DIR = $(TOP)/pmi
PMI_CFLAGS = -I$(PMI_DIR)
PMI_LIBS = -Wl,-rpath,$(PMI_DIR) -L$(PMI_DIR) -lpmi

MPIMOD = mvapich2-gnu-shmem
#MPIMOD = mvapich2-gnu-psm # Hype
#MPIMOD = mvapich2-gnu-ofa # IPA
#MPICC="source /etc/profile.d/modules.sh; module load $(MPIMOD); mpicc"
#MPIRUN=srun

MPICC=mpicc
MPIRUN=mpirun
