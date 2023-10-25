CC=g++
CFLAGS=-std=c++17 -g -Wall -pthread -I./ 
LDFLAGS= -lpthread -ltbb -lhiredis
SUBDIRS=core db redis
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

LIBRARY_GRPC_PATH = `pkg-config --libs --static protobuf grpc++ absl_flags absl_flags_parse` -lsystemd
LIBRARY_GRPCKVS_PATH = -L../gRPC_module
LIBRARY_SS_PATH = -L../SS_no_gmp_module
LIBRARY_HRW_PATH = -L../HRW_hashing_module

LDFLAGS_GRPC = $(LIBRARY_GRPC_PATH)\
           -pthread\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl
LDFLAGS_GRPCKVS = $(LIBRARY_GRPCKVS_PATH) -lgrpckvs
LDFLAGS_SS = $(LIBRARY_SS_PATH) -lshamir_split
LDFLAGS_HRW = $(LIBRARY_HRW_PATH) -lhrw

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_GRPCKVS) $(LDFLAGS_SS) $(LDFLAGS_HRW) $(LDFLAGS_GRPC) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

