#ifndef _RBT_NATARAJAN_EXT_H_
#define _RBT_NATARAJAN_EXT_H_

#include <string.h> /* memset() */
#include <stdint.h> /* uintptr_t */

#include "alloc.h" /* XMALLOC() */

#define CAS(ptr, oldval, newval) \
    __sync_bool_compare_and_swap(ptr, oldval, newval)

typedef uintptr_t word_t;

typedef word_t            pnode_t;   /* Pointer node     */
typedef struct dnode_s    dnode_t;   /* Data node        */
typedef struct val_rec_s  val_rec_t; /* Value record     */
typedef struct op_rec_s   op_rec_t;  /* Operation record */


#define MAX_NTHREADS 8
op_rec_t *ST[MAX_NTHREADS];
op_rec_t *MT[MAX_NTHREADS];

/**
 * One word is a combination of an address pointer and a 2-bit flag.
 * As fas as all adresses are aligned at a 4-byte boundary (e.g. every
 * address has its 2 least significant bits 0) we can use the 2 LSB to store
 * the flag.
 **/
#define ADDR_IS_VALID(addr)     ( ((word_t)(addr) >> 2 << 2) == (word_t)(addr) )
#define WORD_COMBINE_ADDR_FLAG(addr, flag) \
                                ( ((word_t)(addr)) + (flag) )
#define WORD_GET_ADDR(word)     ( (void *)(((word_t)(word)) >> 2 << 2) )
#define WORD_GET_FLAG(word)     ( ((word_t)(word)) & 0x3 )

#define PNODE_TO_DNODE(pnode)   ( (dnode_t *)WORD_GET_ADDR(*pnode) )
#define PNODE_TO_FLAG(pnode)    ( (pnode_flag_t)WORD_GET_FLAG(*pnode) )
#define DNODE_FLAG_TO_PNODE(dnode, flag) \
                                ( (pnode_t)WORD_COMBINE_ADDR_FLAG(dnode, flag) )
#define PNODE_SET_FLAG(pnode, flag) \
                                ( *(pnode) = \
                                  WORD_COMBINE_ADDR_FLAG(WORD_GET_ADDR(*pnode),\
                                                         flag) )

#define STATE_TO_PNODE(state)   ( (pnode_t *)WORD_GET_ADDR(*state) )
#define STATE_TO_DNODE(state)   ( (dnode_t *)WORD_GET_ADDR(*state) )
#define STATE_TO_STATUS(state)  ( (op_status_t)WORD_GET_FLAG(*state) )


/******************************************************************************/
/*                            (a) A pointer node                              */
/******************************************************************************/
#define PNODE_FLAG_STR(pnode) \
    ( PNODE_TO_FLAG(pnode) == FREE ? "FREE" : "OWNED" )

typedef enum {
	FREE = 0,
	OWNED
} pnode_flag_t;

/* Pointer node is word_t 2LSBs are flag, rest are the dnode_t pointer. */

static inline pnode_t *pnode_new(void *addr, pnode_flag_t flag)
{
	pnode_t *ret;

	if (!ADDR_IS_VALID(addr)) {
		fprintf(stderr, "address %p is not a valid word\n", addr);
		exit(1);
	}

	XMALLOC(ret, 1);
	*ret = WORD_COMBINE_ADDR_FLAG(addr, flag);
	return ret;
}
/******************************************************************************/


/******************************************************************************/
/*                            (b) A data node                                 */
/******************************************************************************/
#define IS_RED(dnode) ((dnode)->is_red)
#define IS_BLACK(dnode) (!IS_RED(dnode))
#define COLOR_STR(is_red) ( (is_red) ? "RED" : "BLACK" )

#define IS_EXTERNAL_DNODE(dnode) \
	((dnode)->link[0] == NULL && (dnode)->link[1] == NULL)

#define NEXT_CHILD_PNODE(dnode, _key) \
    ( ((_key) <= (dnode)->key) ? (dnode)->link[0] : (dnode)->link[1] )
#define NEXT_CHILD_DNODE(dnode, key) \
    PNODE_TO_DNODE(NEXT_CHILD_PNODE(dnode, key))
#define OTHER_CHILD_PNODE(dnode, _key) \
    ( ((_key) > (dnode)->key) ? (dnode)->link[0] : (dnode)->link[1] )
#define OTHER_CHILD_DNODE(dnode, key) \
    PNODE_TO_DNODE(OTHER_CHILD_PNODE(dnode, key))

typedef enum {
	WAITING = 0,
	IN_PROGRESS,
	COMPLETED
} op_status_t;

struct dnode_s {
	int is_red;
	int key;
	void *value;
	pnode_t *link[2];

	val_rec_t *valData; /* Unused. */
	op_rec_t *opData;

	word_t next; /* [ move                | status ] */
};

static inline dnode_t *dnode_new(int key, void *value, int is_red)
{
	dnode_t *ret;

	XMALLOC(ret, 1);
	memset(ret, 0, sizeof(*ret));
	ret->is_red = is_red;
	ret->key = key;
	ret->value = value;

	return ret;
}
/******************************************************************************/


/******************************************************************************/
/*                            (c) A value record                              */
/******************************************************************************/
/* FIXME this is unused at the time. */
struct val_rec_s {
	int value;
};
/******************************************************************************/


/******************************************************************************/
/*                          (d) An operation record                           */
/******************************************************************************/
typedef enum {
	SEARCH = 0,
	INSERT,
	DELETE /* unused */
} op_type_t;

struct op_rec_s {
	op_type_t type;
	int key;
	void *value;
	int pid;

	word_t state; /* [ position              | status ] */
};
/******************************************************************************/


/******************************************************************************/
/*                          Red-Black Tree interface                          */
/******************************************************************************/
typedef struct {
	pnode_t *root;
} rbt_t;

//rbt_t *rbt_new();
//int rbt_lookup(/*params_t *params, */rbt_t *rbt, int key);
//int rbt_td_insert_serial_stack(rbt_t *rbt, int key, void *data);
//int rbt_td_delete_serial_stack(rbt_t *rbt, int key);
/******************************************************************************/

#endif /* _RBT_NATARAJAN_EXT_H_ */
