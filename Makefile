CC = gcc
CFLAGS = -Wall -Wextra -g -O3

## Ignore unused variable/parameter warnings.
CFLAGS += -Wno-unused-variable -Wno-unused-parameter
CFLAGS += -Wno-unused-but-set-variable

## Number of transactional retries before resorting to non-tx fallback.
CFLAGS += -DTX_NUM_RETRIES=10

## Which workload do we want?
WORKLOAD_FLAG = -DWORKLOAD_TIME
#WORKLOAD_FLAG = -DWORKLOAD_FIXED
CFLAGS += $(WORKLOAD_FLAG)

INC_FLAGS = -Ilib/
CFLAGS += $(INC_FLAGS)

CFLAGS += -pthread

SOURCE_FILES = main.c lib/clargs.c lib/aff.c bench_pthreads.c

all: pact-ae
pact-ae: rbt avl bst

## Red-Black Trees.
rbt: x.rbt.int.rcu_htm
x.rbt.int.rcu_htm: $(SOURCE_FILES) rbt/rbt_links_bu_int_rcu_htm.c
	$(CC) $(CFLAGS) $^ -o $@

## AVL Trees.
avl: x.avl.int.seq x.avl.int.rcu_htm x.avl.int.rcu_sgl x.avl.int.cop x.avl.bronson
x.avl.int.seq: $(SOURCE_FILES) avl/avl-sequential-internal.c
	$(CC) $(CFLAGS) $^ -o $@
x.avl.int.rcu_htm: $(SOURCE_FILES) avl/avl-rcu-htm-internal.c
	$(CC) $(CFLAGS) $^ -o $@
x.avl.int.rcu_sgl: $(SOURCE_FILES) avl/avl-rcu-htm-internal.c
	$(CC) $(CFLAGS) $^ -o $@ -DTX_NUM_RETRIES=0
x.avl.int.cop: $(SOURCE_FILES) avl/avl-cop-internal.c
	$(CC) $(CFLAGS) $^ -o $@
x.avl.bronson: $(SOURCE_FILES) avl/avl_bronson/avl_bronson_java.c avl/avl_bronson/ssalloc.c
	$(CC) $(CFLAGS) $^ -o $@

## Plain Binary Search Trees (BSTs)
bst: x.bst.aravind x.bst.citrus
x.bst.aravind: $(SOURCE_FILES) bst/bst-aravind.c
	$(CC) $(CFLAGS) $^ -o $@
CITRUS_ORIGINAL_SRC=./lib/citrus
x.bst.citrus: $(SOURCE_FILES) bst/bst-citrus-mine.c $(CITRUS_ORIGINAL_SRC)/new_urcu.c
	$(CC) $(CFLAGS) -I$(CITRUS_ORIGINAL_SRC) $^ -o $@

clean:
	rm -f x.*
