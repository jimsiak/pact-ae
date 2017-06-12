/*   
 *   File: bst-aravind.c
 *   Author: Tudor David <tudor.david@epfl.ch>
 *   Description: Aravind Natarajan and Neeraj Mittal. 
 *   Fast Concurrent Lock-free Binary Search Trees. PPoPP 2014
 *   bst-aravind.c is part of ASCYLIB
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>,
 * 	     	      Tudor David <tudor.david@epfl.ch>
 *	      	      Distributed Programming Lab (LPD), EPFL
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
#include <pthread.h> /* pthread_spinlock_t ... */

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

//#define DO_DRACHSLER_REBALANCE 1
#define DRACHSLER_RO_FAIL RO_FAIL

typedef pthread_spinlock_t lock_t;
#define INIT_LOCK(lock) pthread_spin_init((pthread_spinlock_t *)(lock), PTHREAD_PROCESS_SHARED)
#define LOCK(lock)      pthread_spin_lock((pthread_spinlock_t *)(lock))
#define UNLOCK(lock)    pthread_spin_unlock((pthread_spinlock_t *)(lock))
#define TRYLOCK(lock)   pthread_spin_trylock((pthread_spinlock_t *)(lock))

//typedef pthread_mutex_t lock_t;
//#define INIT_LOCK(lock) pthread_mutex_init((pthread_mutex_t *)(lock), NULL)
//#define LOCK(lock)      pthread_mutex_lock((pthread_mutex_t *)(lock))
//#define UNLOCK(lock)    pthread_mutex_unlock((pthread_mutex_t *)(lock))
//#define TRYLOCK(lock)   pthread_mutex_trylock((pthread_mutex_t *)(lock))

#define TRUE 1
#define FALSE 0

#define MAX_KEY INT_MAX
#define MIN_KEY -1

#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

typedef char bool_t;
typedef int skey_t;
typedef void * sval_t;

typedef struct node_s {
	volatile struct node_s *left,
	                       *right,
	                       *parent,
	                       *succ,
	                       *pred;

#	ifdef DO_DRACHSLER_REBALANCE
	int32_t left_height,
	        right_height;
#	endif

	lock_t tree_lock,
	       succ_lock;

	skey_t key;
	sval_t value;
	bool_t mark;

//	char padding[CACHE_LINE_SIZE - 5 * sizeof(volatile struct node_s *) -
//	             #ifdef DO_DRACHSLER_REBALANCE
//	             2 * sizeof(int32_t) -
//	             #endif
//	             2 * sizeof(lock_t) -
//	             sizeof(skey_t) - sizeof(sval_t) - sizeof(bool_t)];
//} __attribute__((packed)) __attribute__((aligned(CACHE_LINE_SIZE))) node_t;
} node_t;

node_t *create_node(skey_t k, sval_t value) {
	volatile node_t* new_node;

	XMALLOC(new_node, 1);

	new_node->left = NULL;
	new_node->right = NULL;
	new_node->parent = NULL;
	new_node->succ = NULL;
	new_node->pred = NULL;
	INIT_LOCK(&new_node->tree_lock);
	INIT_LOCK(&new_node->succ_lock);
	new_node->key = k;
	new_node->value = value;
	new_node->mark = FALSE;
	asm volatile("" ::: "memory");
	return (node_t*) new_node;
}

node_t *initialize_tree() {
	node_t *parent, *root;

	parent = create_node(MIN_KEY, 0);
	root = create_node(MAX_KEY, 0);

    root->pred = parent;
    root->succ = parent;
    root->parent = parent;
    parent->right = root;
    parent->succ = root;
    return root;
}

node_t *bst_search(skey_t k, node_t *root) {
	node_t* n = root;
	node_t* child;
	skey_t curr_key;

	while (1) {
		curr_key = n->key;
		if (curr_key == k)
			return n;
		if ( curr_key < k )
			child = (node_t*) n->right;
		else
			child = (node_t*) n->left;

		if ( child == NULL )
			return n;
		n = child;
	}
}

int bst_contains(skey_t k, node_t* root) {
	node_t* n = bst_search(k,root);
	while (n->key > k) n = (node_t*) n->pred;
	while (n->key < k) n = (node_t*) n->succ;
	return ((n->key == k) && (n->mark == FALSE));
}

node_t* choose_parent(node_t* p, node_t* s, node_t* first_cand) {
	node_t* candidate;
	if ((first_cand == p) || (first_cand == s))
		candidate = first_cand;
	else
		candidate = p;

	while (1) {
		LOCK(&candidate->tree_lock);
		if (candidate == p) {
			if (candidate->right == NULL)
				return candidate;

			UNLOCK(&candidate->tree_lock);
			candidate = s;
		} else {
			if (candidate->left == NULL)
				return candidate;

			UNLOCK(&candidate->tree_lock);
			candidate = p;
		}
	}
}

void insert_to_tree(node_t* parent, node_t* new_node, node_t* root) {
	new_node->parent = parent;
	if (parent->key < new_node->key)
		parent->right = new_node;
	else
		parent->left = new_node;

	UNLOCK(&parent->tree_lock);
}

int bst_insert(skey_t k, sval_t v, node_t* root) {
	while(1) {
		node_t* node = bst_search(k, root);
		volatile node_t* p;
		if (node->key >= k) p = (node_t*) node->pred;
		else                p = (node_t*) node;

#		if DRACHSLER_RO_FAIL == 1
		node_t* n = node;
		while (n->key > k)
			n = (node_t*) n->pred;
		while (n->key < k)
			n = (node_t*) n->succ;
		if ((n->key == k) && (n->mark == FALSE)) 
			return 0;
#endif

		LOCK(&p->succ_lock);
		volatile node_t* s = (node_t*) p->succ;
		if ((k > p->key) && (k <= s->key) && (p->mark == FALSE)) {
			if (s->key == k) {
				UNLOCK(&p->succ_lock);
				return 0;
			}

			node_t* new_node = create_node(k,v);
			node_t* parent = choose_parent((node_t*) p, (node_t*) s, node);
			new_node->succ = s;
			new_node->pred = p;
			new_node->parent = parent;
//			__sync_synchronize();

			s->pred = new_node;
			p->succ = new_node;
			UNLOCK(&p->succ_lock);
			insert_to_tree((node_t*) parent,(node_t*) new_node,(node_t*) root);
			return 1;
		}
		UNLOCK(&p->succ_lock);
	}
}

node_t* lock_parent(node_t* node) {
	node_t* p;
	while (1) {
		p = (node_t*) node->parent;
		LOCK(&p->tree_lock);
		if ((node->parent == p) && (p->mark == FALSE))
			return p;
		UNLOCK(&p->tree_lock);
	}
}

bool_t acquire_tree_locks(node_t* n) {
	while (1) {
		LOCK(&n->tree_lock);
		node_t* left = (node_t*) n->left;
		node_t* right = (node_t*) n->right;
		//lock_parent(n);
		if ((right == NULL) || (left == NULL)) {
			return FALSE;
		} else {
			node_t* s = (node_t*) n->succ;
			int l=0;

			node_t* parent;
			node_t* sp = (node_t*) s->parent;
			if (sp != n) {
				parent = sp;
//				if (!TRYLOCK(&(parent->tree_lock))) { // FIXME
				if (TRYLOCK(&(parent->tree_lock)) != 0) { // FIXME
					UNLOCK(&(n->tree_lock));
					//UNLOCK(&(n->parent->tree_lock));
					continue;
				}
				l=1;
				if ((parent != s->parent) || (parent->mark==TRUE)) {
					UNLOCK(&(n->tree_lock));
					UNLOCK(&(parent->tree_lock));
					//UNLOCK(&(n->parent->tree_lock));
					continue;
				}
			}

//			if (!TRYLOCK(&(s->tree_lock))) {
			if (TRYLOCK(&(s->tree_lock)) != 0) {
				UNLOCK(&(n->tree_lock));
				//UNLOCK(&(n->parent->tree_lock));
				if (l) 
					UNLOCK(&(parent->tree_lock));
				continue;
			}

			return TRUE;
		}
	}
}

void update_child(node_t* parent, node_t* old_ch, node_t* new_ch) {
	if (parent->left == old_ch)
		parent->left = new_ch;
	else
		parent->right = new_ch;

	if (new_ch != NULL)
		new_ch->parent = parent;
}

void remove_from_tree(node_t* n, bool_t has_two_children,node_t* root) {
	node_t *child, *parent, *s;

    //int l=0;
	if (has_two_children == FALSE) {
		if ( n->right == NULL)
			child = (node_t*) n->left;
		else
			child = (node_t*) n->right;
		parent = (node_t*) n->parent;
		update_child(parent, n, child);
	} else {
		s = (node_t*) n->succ;
		child = (node_t*) s->right;
		parent = (node_t*) s->parent;
		//if (parent != n ) l=1;
		update_child(parent, s, child);
		s->left = n->left;
		s->right = n->right;
		n->left->parent = s;
		if (n->right != NULL)
			n->right->parent = s;
		update_child((node_t*) n->parent, n, s);
		if (parent == n)
			parent = s;
		else
			UNLOCK(&(s->tree_lock));
		UNLOCK(&(parent->tree_lock));
	}
	UNLOCK(&(n->parent->tree_lock));
	UNLOCK(&(n->tree_lock));
}



int bst_remove(skey_t k, node_t* root) {
	node_t* node;

	while (1) {
		node = bst_search(k, root);
		node_t* p;
		if (node->key >= k)
			p = (node_t*) node->pred;
		else
			p = (node_t*) node;

#		if DRACHSLER_RO_FAIL == 1
		node_t* n = node;
		while (n->key > k)
			n = (node_t*) n->pred;
		while (n->key < k)
			n = (node_t*) n->succ;
		if ((n->key != k) && (n->mark == FALSE)) 
			return 0;
#		endif

		LOCK(&p->succ_lock);
		node_t* s = (node_t*) p->succ;
		if ((k > p->key) && (k <= s->key) && (p->mark == FALSE)) {
			if (s->key > k) {
				UNLOCK(&p->succ_lock);
				return 0;
			}
			LOCK(&s->succ_lock);
			bool_t has_two_children = acquire_tree_locks(s);
			lock_parent(s);
			s->mark = TRUE;
			node_t* s_succ = (node_t*) s->succ;
			s_succ->pred = p;
			p->succ = s_succ;
			UNLOCK(&s->succ_lock);
			UNLOCK(&p->succ_lock);
			sval_t v = s->value;
			remove_from_tree(s, has_two_children,root);
			return 1; 
		}
		UNLOCK(&p->succ_lock);
	}
}

unsigned long long bst_size(volatile node_t* node) {
	if (!node) 
		return 0;

    unsigned long long x = 0;
    if ((node->key != MAX_KEY) && (node->key != MIN_KEY))
        x = 1;

	return x + bst_size((node_t*) node->right) + bst_size((node_t*) node->left);
}

static inline int _bst_warmup_helper(node_t *root, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;

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

	if (root->key < MAX_KEY && root->key > MIN_KEY) {
		total_nodes++;
		_th++;
	}

	/* BST violation? */
	if (left && left->key >= root->key)
		bst_violations++;
	if (right && right->key <= root->key)
		bst_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if ((!left || !right) && (root->key != MAX_KEY && root->key != MIN_KEY)) {
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
	ret = bst_contains(key, bst);
	return ret;
}

int rbt_insert(void *bst, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = bst_insert(key, value, bst);
	return ret;
}

int rbt_delete(void *bst, void *thread_data, int key)
{
	int ret = 0;
	ret = bst_remove(key, bst);
	return ret;
}

int rbt_validate(void *bst)
{
	int ret = 1;
	volatile node_t *root = bst;
	ret = _bst_validate_helper(root);
	return ret;
}

int rbt_warmup(void *bst, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _bst_warmup_helper(bst, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "bst_drachsler";
}
