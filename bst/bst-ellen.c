/*   
 *   File: bst-aravind.c
 *   Author: Tudor David <tudor.david@epfl.ch>
 *   Description: non-blocking binary search tree
 *      based on "Non-blocking Binary Search Trees"
 *      F. Ellen et al., PODC 2010
 *   bst_ellen.c is part of ASCYLIB
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>,
 *                Tudor David <tudor.david@epfl.ch>
 *                Distributed Programming Lab (LPD), EPFL
 *
 * ASCYLIB is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 */

#include <stdint.h>
#include <stdlib.h>
#include <limits.h>

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

#define MEM_BARRIER __sync_synchronize()
#define CAS_PTR(a,b,c) __sync_val_compare_and_swap(a,b,c)

// The states a node can have: we avoid an enum to better control the size of
//                             the data structures
#define STATE_CLEAN 0
#define STATE_DFLAG 1
#define STATE_IFLAG 2
#define STATE_MARK 3

#define TRUE 1
#define FALSE 0

#define MIN_KEY 0
#define MAX_KEY INT_MAX
#define INF2 (MAX_KEY)
#define INF1 (MAX_KEY - 1)

#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

typedef int skey_t;
typedef void * sval_t;
typedef unsigned char leaf_t;
typedef unsigned char bool_t;

typedef struct node_t node_t;
typedef union info_t info_t;

typedef info_t* update_t; // FIXME quite a few CASes done on this data
                          //       structure; make it cache line aligned?

typedef struct iinfo_t {
	node_t* p;
	node_t* new_internal;
	node_t* l;
} iinfo_t;

typedef struct dinfo_t {
	node_t* gp;
	node_t* p;
	node_t* l;
	update_t pupdate;
} dinfo_t;

union info_t {
	iinfo_t iinfo;
	dinfo_t dinfo;
	uint8_t padding[CACHE_LINE_SIZE];
};

struct node_t {
	skey_t key;
	sval_t value;
	update_t update;
	volatile node_t* left;
	volatile node_t* right;
	bool_t leaf;
//	char padding[CACHE_LINE_SIZE - sizeof(sval_t) - sizeof(skey_t) -
//	             sizeof(update_t) - 2*sizeof(uintptr_t) - sizeof(bool_t)];
//} __attribute__((packed)) __attribute__((aligned(CACHE_LINE_SIZE)));
};

typedef struct search_result_t {
	node_t* gp; 
	node_t* p;
	node_t* l;
	update_t pupdate;
	update_t gpupdate;
#ifdef DO_PAD
	char padding[24];
#endif
} search_result_t;

__thread search_result_t *last_result;

int bst_cas_child(node_t* parent, node_t* old, node_t* new);
void bst_help(update_t u);
void bst_help_marked(info_t* op);
bool_t bst_help_delete(info_t* op);
void bst_help_insert(info_t * op);
int bst_find(skey_t key, node_t* root);
int bst_insert(skey_t key, sval_t value, node_t* root);
int bst_delete(skey_t key, node_t* root);
search_result_t* bst_search(skey_t key, node_t* root);

static inline uint64_t GETFLAG(update_t ptr) {
	return ((uint64_t)ptr) & 3;
}

static inline uint64_t FLAG(update_t ptr, uint64_t flag) {
	return (((uint64_t)ptr) & 0xfffffffffffffffc) | flag;
}

static inline uint64_t UNFLAG(update_t ptr) {
	return (((uint64_t)ptr) & 0xfffffffffffffffc);
}

node_t* create_node(skey_t key, sval_t value, bool_t is_leaf) {
	volatile node_t * new_node;

	XMALLOC(new_node, 1);

	new_node->leaf = is_leaf;
	new_node->key = key;
	new_node->value = value;
	new_node->update = NULL;
	new_node->right = NULL;
	new_node->left = NULL;

	asm volatile("" ::: "memory");

	return (node_t*) new_node;
}

node_t *initialize_tree() {
	node_t *root, *i1, *i2;

	root = create_node(INF2,0,FALSE);
	i1 = create_node(INF1,0,TRUE);
	i2 = create_node(INF2,0,TRUE);
  
	root->left = i1;
	root->right = i2;
	
	return root;
}

void bst_init_local() {
	last_result = (search_result_t*) malloc(sizeof(search_result_t));
}

search_result_t *bst_search(skey_t key, node_t* root) {
	volatile search_result_t res;

	res.l = root;
	while (!(res.l->leaf)) {
		res.gp = res.p;
		res.p = res.l;
		res.gpupdate = res.pupdate;
		res.pupdate = res.p->update;
		if (key < res.l->key) {
			res.l = (node_t*) res.p->left;
		} else {
			res.l = (node_t*) res.p->right;
		}
	
	}
	last_result->gp=res.gp;
	last_result->p=res.p;
	last_result->l=res.l;
	last_result->gpupdate=res.gpupdate;
	last_result->pupdate=res.pupdate;
	return last_result;
}

int bst_find(skey_t key, node_t* root) {
	search_result_t * result = bst_search(key,root);
	if (result->l->key == key) {
		return 1;
	}
	return 0;
}

info_t* create_iinfo_t(node_t* p, node_t* ni, node_t* l){
	volatile info_t * new_info;

	XMALLOC(new_info, 1);

	new_info->iinfo.p = p; 
	new_info->iinfo.new_internal = ni; 
	new_info->iinfo.l = l; 
	MEM_BARRIER;
	return (info_t*) new_info;
}

info_t* create_dinfo_t(node_t* gp, node_t* p, node_t* l, update_t u){
	volatile info_t * new_info;

	XMALLOC(new_info, 1);

	new_info->dinfo.gp = gp; 
	new_info->dinfo.p = p; 
	new_info->dinfo.l = l; 
	new_info->dinfo.pupdate = u; 
	MEM_BARRIER;
	return (info_t*) new_info;
}

int bst_cas_child(node_t* parent, node_t* old, node_t* new){
	if (new->key < parent->key) {
		if (CAS_PTR(&(parent->left),old,new) == old) return 1;
		return 0;
	} else {
		if (CAS_PTR(&(parent->right),old,new) == old) return 1;
		return 0;
	}
}

void bst_help_insert(info_t * op) {
	int i = bst_cas_child(op->iinfo.p,op->iinfo.l,op->iinfo.new_internal);
	void* dummy = CAS_PTR(&(op->iinfo.p->update),FLAG(op,STATE_IFLAG),FLAG(op,STATE_CLEAN));
}

int bst_insert(skey_t key, sval_t value,  node_t* root) {
	node_t *new_internal, *new_sibling, *new_node = NULL;
	update_t result;
	info_t* op;
	search_result_t* search_result;

	while(1) {
		search_result = bst_search(key,root);
		if (search_result->l->key == key)
			return 0;

		if (GETFLAG(search_result->pupdate) != STATE_CLEAN) {
			bst_help(search_result->pupdate);
		} else {
			if (new_node==NULL)
				new_node = create_node(key, value, TRUE); 

			new_sibling = create_node(search_result->l->key, search_result->l->value, TRUE);
			new_internal = create_node(max(key,search_result->l->key), 0, FALSE);

			if (new_node->key < new_sibling->key) {
				new_internal->left = new_node;
				new_internal->right = new_sibling;
			} else {
				new_internal->left = new_sibling;
				new_internal->right = new_node;
			}
			op = create_iinfo_t(search_result->p, new_internal, search_result->l);
			result = CAS_PTR(&(search_result->p->update),search_result->pupdate,FLAG(op,STATE_IFLAG));
			if (result == search_result->pupdate) {
				bst_help_insert(op);
				return 1;
			} else {
				bst_help(result);
			}
		}
	}
}

bool_t bst_help_delete(info_t* op) {
	update_t result; 
	result = CAS_PTR(&(op->dinfo.p->update), op->dinfo.pupdate, FLAG(op,STATE_MARK));
	if ((result == op->dinfo.pupdate) || (result == ((info_t*)FLAG(op,STATE_MARK)))) {
		bst_help_marked(op);
		return TRUE;
	} else {
		bst_help(result);
		void* dummy = CAS_PTR(&(op->dinfo.gp->update), FLAG(op,STATE_DFLAG), FLAG(op,STATE_CLEAN));
		return FALSE;
	}
}

void bst_help_marked(info_t* op) {
	node_t* other;
	if (op->dinfo.p->right == op->dinfo.l) {
		other = (node_t*) op->dinfo.p->left;
	} else {
		other = (node_t*) op->dinfo.p->right; 
	}
	int i = bst_cas_child(op->dinfo.gp,op->dinfo.p,other);
	void* dummy = CAS_PTR(&(op->dinfo.gp->update), FLAG(op,STATE_DFLAG),FLAG(op,STATE_CLEAN));
}

void bst_help(update_t u){
	if (GETFLAG(u) == STATE_IFLAG) {
		bst_help_insert((info_t*) UNFLAG(u));
	} else if (GETFLAG(u) == STATE_MARK) {
		bst_help_marked((info_t*) UNFLAG(u));
	} else if (GETFLAG(u) == STATE_DFLAG) {
		bst_help_delete((info_t*) UNFLAG(u)); 
	}
}

int bst_delete(skey_t key, node_t* root) {
	update_t result;
	info_t* op;
	sval_t found_value; 

	search_result_t* search_result;

	while (1) {
		search_result = bst_search(key,root); 
		if (search_result->l->key!=key) {
			return 0;
		}
		found_value=search_result->l->value;
		if (GETFLAG(search_result->gpupdate)!=STATE_CLEAN) {
			bst_help(search_result->gpupdate);
		} else if (GETFLAG(search_result->pupdate)!=STATE_CLEAN){
			bst_help(search_result->pupdate);
		} else {
			op = create_dinfo_t(search_result->gp, search_result->p, search_result->l,search_result->pupdate);
			result = CAS_PTR(&(search_result->gp->update),search_result->gpupdate,FLAG(op,STATE_DFLAG));
			if (result == search_result->gpupdate) {
				if (bst_help_delete(op)==TRUE) {
					return 1;
				}
			} else {
				bst_help(result);
			}
		}
	}
}

unsigned long long bst_size_rec(node_t* node){
		if (node->leaf==FALSE) {
			return (bst_size_rec((node_t*) node->right) + bst_size_rec((node_t*) node->left));
		} else {
			return 1;
		}
}

unsigned long long bst_size(node_t* node){
	return bst_size_rec(node)-2;
}

static inline int _bst_warmup_helper(node_t *root, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;

	bst_init_local();

	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;

		ret = bst_insert(key, NULL, root);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}

static int total_paths, total_nodes, bst_violations;
static int min_path_len, max_path_len;
static void _bst_validate_rec(volatile node_t *root, int _th)
{
	if (!root)
		return;

	volatile node_t *left = root->left;
	volatile node_t *right = root->right;

	if (root->key < INF1) {
		total_nodes++;
		_th++;
	}

	/* BST violation? */
	if (left && left->key >= root->key)
		bst_violations++;
	if (right && right->key < root->key)
		bst_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if (!left && !right && root->key < INF1) {
		total_paths++;

		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* Check subtrees. */
	if (left)
		_bst_validate_rec(left, _th);
	if (right)
		_bst_validate_rec(right, _th);
}

static inline int _bst_validate_helper(volatile node_t *root)
{
	int check_bst = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	bst_violations = 0;

	_bst_validate_rec(root, 0);

	check_bst = (bst_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size: %8d\n", total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_bst;
}

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
	printf("Size of tree node is %lu\n", sizeof(node_t));
	return (void *)initialize_tree();
}

void *rbt_thread_data_new(int tid)
{
	bst_init_local();
//	return htm_fg_tdata_new(tid);
	return NULL;
}

void rbt_thread_data_print(void *thread_data)
{
//	htm_fg_tdata_print(thread_data);
	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
//	htm_fg_tdata_add(d1, d2, dst);
}

int rbt_lookup(void *bst, void *thread_data, int key)
{
	int ret;
	ret = bst_find(key, bst);
	return ret;
}

int rbt_insert(void *avl, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = bst_insert(key, value, avl);
	return ret;
}

int rbt_delete(void *avl, void *thread_data, int key)
{
	int ret = 0;
	ret = bst_delete(key, avl);
	return ret;
}

int rbt_validate(void *avl)
{
	int ret = 1;
	volatile node_t *root = avl;
	ret = _bst_validate_helper(root);
	return ret;
}

int rbt_warmup(void *avl, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _bst_warmup_helper(avl, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "bst_ellen";
}
