#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <xmmintrin.h>

#include "rtm.h"
//data = key//

struct jsw_node {
  int red;
  int data;
  struct jsw_node *link[2];
  int lock; //0=free, 1=locked//
};

struct jsw_tree {
  struct jsw_node *root;
  pthread_spinlock_t spinlock;
};

#define IS_BLACK(node) ( !(node) || !(node)->red )
#define IS_RED(node) ( !IS_BLACK(node) )

int is_red ( struct jsw_node *root ){
  return root != NULL && root->red == 1;
}

struct jsw_node *jsw_single ( struct jsw_node *root, int dir, struct jsw_node *save ){

  save = root->link[!dir];

  root->link[!dir] = save->link[dir];
  save->link[dir] = root;

  root->red = 1;
  save->red = 0;

  return save;
}

struct jsw_node *jsw_double ( struct jsw_node *root, int dir, struct jsw_node *save ){
  root->link[!dir] = jsw_single ( root->link[!dir], !dir, save );
  return jsw_single ( root, dir, save );
}

struct jsw_node *make_node ( int data ){
  struct jsw_node *rn = malloc ( sizeof *rn );

   if ( rn != NULL ) {
    rn->data = data;
    rn->red = 1; /* 1 is red, 0 is black */
    rn->link[0] = NULL;
    rn->link[1] = NULL;
	rn->lock = 0;
  }

  return rn;
}

int jsw_insert2(struct jsw_tree *tree, int data) {
	struct jsw_node *save = NULL;
	int inserted = 0;

	if ( tree->root == NULL ) {
		/* Empty tree case */
		tree->root = make_node ( data );
		if ( tree->root == NULL )
			return 0;
		inserted = 1;
	} else {
		struct jsw_node head = {0}; /* False tree root */
		
		struct jsw_node *g, *t;     /* Grandparent & parent */
		struct jsw_node *p, *q;     /* Iterator & parent */
		int dir = 0, last;
		
		/* Set up helpers */
		t = &head;
		g = p = NULL;
		q = t->link[1] = tree->root;
		
		/* Search down the tree */
		for ( ; ; ) {
			if ( q == NULL ) {
				/* Insert new node at the bottom */
				p->link[dir] = q = make_node ( data );
				inserted = 1;
				if ( q == NULL )
					return 0;
			} else if ( is_red ( q->link[0] ) && is_red ( q->link[1] ) ) {
				/* Color flip */
				q->red = 1;
				q->link[0]->red = 0;
				q->link[1]->red = 0;
			}
			
			/* Fix red violation */
			if ( is_red ( q ) && is_red ( p ) ) {
				int dir2 = t->link[1] == g;
				
				if ( q == p->link[last] )
					t->link[dir2] = jsw_single ( g, !last, save );
				else
					t->link[dir2] = jsw_double ( g, !last, save );
			}
			
			/* Stop if found */
			if ( q->data == data )
				break;
			
			last = dir;
			dir = q->data < data;
			
			/* Update helpers */
			if ( g != NULL )
				t = g;
			g = p, p = q;
			q = q->link[dir];
		}
		
		/* Update root */
		tree->root = head.link[1];
	}
	
	/* Make root black */
	tree->root->red = 0;
	
	return inserted;
}

int jsw_insert ( struct jsw_tree *tree, int data, long int *aborts,pthread_spinlock_t *lock ){
  int count=0,count2=0;
  struct jsw_node *save = NULL;
  struct jsw_node *z = make_node ( data );
  struct jsw_node head = {0}; /* False tree root */
  struct jsw_node *g, *t;     /* Grandparent & parent */
  struct jsw_node *p, *q;     /* Iterator & parent */
  int dir = 0, dir2, last, placed=0;
  struct timespec tim, tim2;
  tim.tv_sec = 0;
  tim.tv_nsec = 1;
  start_over:
  //State that you are suppose to use the root - make initializations//
  while(1){
  
	unsigned status = _xbegin();
	if (status == _XBEGIN_STARTED) {
		if (((int)(*lock)) != 1) _xabort (0xfe);
		if (tree->root == NULL){
			tree->root = z;
			tree->root->red = 0;
			_xend();
			return (1);
		}	
		if (tree->root->lock == 1) 
			_xabort(0xff);
		tree->root->lock = 1;
		t = &head;
		g = p = NULL;
		q = t->link[1] = tree->root;
		_xend();
		break;
	}
	else{
		if (status & _XABORT_CONFLICT)
		{
			aborts[0]++;
			count++;
			count2++;
			if (count>9){
				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);			
		}
		else if (status & _XABORT_CAPACITY){
			aborts[1]++;			
			count2++;
			if (count2<0)
			 return(-2);			
		}	
		else if (status & _XABORT_EXPLICIT)
		{
			aborts[2]++;
			if (_XABORT_CODE(status) == 0xfe)
				goto start_over;
			count++;
			count2++;
			if (count>9){
				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);
		}
		else{
			aborts[3]++;
			count2++;
			if (count2<0)
			 return(-2);
		}
	}   
  } 
  
//fprintf(stderr, "got the first lock\n");
count = 0;
count2 = 0;
 while(1){
	
	unsigned status = _xbegin();
	if (status == _XBEGIN_STARTED){
	  if (((int)(*lock)) != 1) _xabort (0xfe);	
	  /* Search down the tree */
      if ( q == NULL ) {
        /* Insert new node at the bottom */
		placed=1;
        p->link[dir] = q = z;	

      }
      else if ( is_red ( q->link[0] ) && is_red ( q->link[1] ) ) {
        /* Color flip */
        q->red = 1;
        q->link[0]->red = 0;
        q->link[1]->red = 0;
      }
		
      /* Fix red violation */
      if ( is_red ( q ) && is_red ( p ) ) {
        dir2 = t->link[1] == g;

         if ( q == p->link[last] ){
          t->link[dir2] = jsw_single ( g, !last, save );
        }
		else{
          t->link[dir2] = jsw_double ( g, !last, save );
		}
	  }
		
      /* Stop if found */
      if ( q->data == data ){
	    if (q!=NULL)
			q->lock = 0;
		if (t!=NULL)
			t->lock = 0;
		if (g!=NULL)
			g->lock = 0;
        if (p!=NULL)
			p->lock = 0;
		if (t==&head){
			tree->root = head.link[1]; 
			tree->root->red = 0;
		}
	      
		_xend();
		if (placed == 0) free(z);
		return 1;
	  }
      
	  last = dir;
      dir = q->data < data;
	  
	  //free the lock for the current window and take for the next one// 
	  if (q!=NULL)
		q->lock = 0;
	  if (t!=NULL)
		t->lock = 0;
	  if (g!=NULL)
		g->lock = 0;
      if (p!=NULL)
		p->lock = 0;
		
	  if ((t==&head)||(g==&head)){
		tree->root = head.link[1]; 
		tree->root->red = 0;
	  }
	  
	  /* Update helpers */
	  //fprintf(stderr,"I was here2 \n");
      if ( g != NULL )
        t = g;
      g = p, p = q;
      q = q->link[dir];
	  
	  if(q!=NULL)
		if (q->lock ==1)
			_xabort(0xff);
	  if(t!=NULL)
		if (t->lock ==1)
			_xabort(0xff);
	  if(g!=NULL)
		if (g->lock ==1)
			_xabort(0xff);
	  if(p!=NULL)
		if (p->lock ==1)
			_xabort(0xff);		
	  
	  
	  if (q!=NULL)
		q->lock = 1;
	  if (t!=NULL)
		t->lock = 1;
	  if (g!=NULL)
		g->lock = 1;
      if (p!=NULL)
		p->lock = 1;
	  

	  _xend();
	  count2 = 0;
	  count = 0;
    }
	else{
		if (status & _XABORT_CONFLICT)
		{
			aborts[0]++;
			count++;
			count2++;
			if (count>9){
				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);			
		}
		else if (status & _XABORT_CAPACITY){
			aborts[1]++;			
			count2++;
			if (count2<0)
			 return(-2);			
		}	
		else if (status & _XABORT_EXPLICIT)
		{
			aborts[2]++;
			if (_XABORT_CODE(status) == 0xfe)
				goto start_over;
			count++;
			count2++;
			if (count>9){
				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);
		}
		else{
			aborts[3]++;
			count2++;
			if (count2<0)
			 return(-2);
		}
	}    
  }
}



int jsw_remove2 ( struct jsw_tree *tree, int data ){
  if ( tree->root != NULL ) {
    struct jsw_node head = {0}; /* False tree root */
    struct jsw_node *q, *p, *g, *save=NULL; /* Helpers */
    struct jsw_node *f = NULL;  /* Found item */
    int dir = 1;

    /* Set up helpers */
    q = &head;
    g = p = NULL;
    q->link[1] = tree->root;

    /* Search and push a red down */
    while ( q->link[dir] != NULL ) {
      int last = dir;

      /* Update helpers */
      g = p, p = q;
      q = q->link[dir];
      dir = q->data < data;

      /* Save found node */
      if ( q->data == data )
        f = q;

      /* Push the red node down */
      if ( !is_red ( q ) && !is_red ( q->link[dir] ) ) {
        if ( is_red ( q->link[!dir] ) )
          p = p->link[last] = jsw_single ( q, dir, save );
        else if ( !is_red ( q->link[!dir] ) ) {
          struct jsw_node *s = p->link[!last];

          if ( s != NULL ) {
            if ( !is_red ( s->link[!last] ) && !is_red ( s->link[last] ) ) {
             /* Color flip */
              p->red = 0;
              s->red = 1;
              q->red = 1;
            }
            else {
              int dir2 = g->link[1] == p;

              if ( is_red ( s->link[last] ) )
                g->link[dir2] = jsw_double ( p, last, save );
              else if ( is_red ( s->link[!last] ) )
                g->link[dir2] = jsw_single ( p, last, save );

              /* Ensure correct coloring */
              q->red = g->link[dir2]->red = 1;
              g->link[dir2]->link[0]->red = 0;
              g->link[dir2]->link[1]->red = 0;
            }
          }
        }
      }
    }

    /* Replace and remove if found */
    if ( f != NULL ) {
      f->data = q->data;
      p->link[p->link[1] == q] =
        q->link[q->link[0] == NULL];
      free ( q );
    }

    /* Update root and make it black */
    tree->root = head.link[1];
    if ( tree->root != NULL )
      tree->root->red = 0;
  }

  return 1;
}

int jsw_remove ( struct jsw_tree *tree, int data, long int *aborts,pthread_spinlock_t *lock ){
 struct jsw_node *save=NULL;
 struct jsw_node head = {0}; /* False tree root */
 struct jsw_node *q, *p=NULL, *g=NULL; /* Helpers */
 struct jsw_node *f = NULL;  /* Found item */
 struct jsw_node  *q1, *p1, *g1,*qlink1,*qlink0,*plink1,*plink0;
 int dir = 1;
 int last = dir; 
 int count = 0, count2=0; 
 struct timespec tim, tim2;
 tim.tv_sec = 0;
 tim.tv_nsec = 1; 
 start_over:
 while(1){
	unsigned status = _xbegin();
	if (status == _XBEGIN_STARTED){
		if (((int)(*lock)) != 1) _xabort (0xfe);
		if (tree->root == NULL){
			_xend();
			return 1;
		}	
		if (tree->root->lock == 1) 
			_xabort(0xff);			
		tree->root->lock = 1;	

		
		q = &head;
		g = p = NULL;
		q->link[1] = tree->root;
		
		g = p, p = q;
		q = q->link[dir];
		dir = q->data < data;
		last = 1;
		
		_xend();
		break;
	}
	else{
		if (status & _XABORT_CONFLICT)
		{
			aborts[0]++;
			count++;
			count2++;
			if (count>9){
//				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);			
		}
		else if (status & _XABORT_CAPACITY){
			aborts[1]++;			
			count2++;
			if (count2<0)
			 return(-2);			
		}	
		else if (status & _XABORT_EXPLICIT)
		{
			aborts[2]++;
			if (_XABORT_CODE(status) == 0xfe)
				goto start_over;
			count++;
			count2++;
			if (count>9){
//				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);
		}
		else{
			aborts[3]++;
			count2++;
			if (count2<0)
			 return(-2);
		}
	}    
  }
  
  
  count = 0;
  count2 = 0;
  //q->link[0]->lock = 1;
  while(1){

	unsigned status = _xbegin();
	if (status == _XBEGIN_STARTED){
	  
	  if (((int)(*lock)) != 1) _xabort (0xfe);
				  	
	 // fprintf(stderr, "here1\n");
      /* Search and push a red down */

 	  if (p!=NULL)
		if (p->link[!last]!=NULL)
			if(p->link[!last]->lock==1)
				_xabort(0xff);	
	  
	  q1=q;
	  p1=p;
	  g1=g;

      //if (found == 1)
		//f->lock = 1;		
	 // fprintf(stderr, "here2\n");
	
      /* Save found node */
	  if (q->data == data){
        f = q;
	  }

	  
      /* Push the red node down */
      if ( !is_red ( q ) && !is_red ( q->link[dir] ) ) {
        if ( is_red ( q->link[!dir] ) )
          p = p->link[last] = jsw_single ( q, dir, save );
        else if ( !is_red ( q->link[!dir] ) ) {
          struct jsw_node *s = p->link[!last];

          if ( s != NULL ) {
            if ( !is_red ( s->link[!last] ) && !is_red ( s->link[last] ) ) {
             /* Color flip */
              p->red = 0;
              s->red = 1;
              q->red = 1;
            }
            else {
              int dir2 = g->link[1] == p;

              if ( is_red ( s->link[last] ) )
                g->link[dir2] = jsw_double ( p, last, save );
              else if ( is_red ( s->link[!last] ) )
                g->link[dir2] = jsw_single ( p, last, save );

              /* Ensure correct coloring */
              q->red = g->link[dir2]->red = 1;
              g->link[dir2]->link[0]->red = 0;
              g->link[dir2]->link[1]->red = 0;
            }
          }
        }
      }
      
	  
	  
	  //Release the atomic lock for the current window and take for the next one!//
	  if ((g==&head)||(p==&head)||(g==NULL)||(p==NULL)){
		tree->root = head.link[1];
		tree->root->red = 0;
	  }
	  
	  
	  if ( q->link[dir] == NULL ){
	 
	  
	  
	 // fprintf(stderr, "found\n");
	  
	 if(q1!=NULL){	
		q1->lock = 0;
	  }
	  if (g1!=NULL)
		g1->lock=0;
	  if (p1!=NULL){
		p1->lock=0;
	  }	

	 // fprintf(stderr, "found\n");		
		if ( f != NULL ) {
			//f->lock=0;
			f->data=q->data;
			p->link[p->link[1] == q] =
				q->link[q->link[0] == NULL];
			
		}  
		
	 // fprintf(stderr, "found\n");
		_xend();
		if ( f != NULL ) free(q);
		return 1;
	  }
	  //tree->root->red = 0;
	  //fprintf(stderr, "here3\n");
	  

	  

	  last = dir;

	  if(q1!=NULL)
	  {	
		q1->lock = 0;
	  }
	  if (g1!=NULL)
		g1->lock=0;
	  if (p1!=NULL){
		p1->lock=0;
	  }	

		
	 /* Update helpers */
	  g = p, p = q;
      q = q->link[dir];
      dir = q->data < data;
	

	  
	
	  if(q!=NULL){
		if (q->lock ==1)
			_xabort(0xff);	
	  }
	  if(g!=NULL)
		if (g->lock ==1)
			_xabort(0xff);
	  if(p!=NULL){
		if (p->lock ==1)
			_xabort(0xff);
	  }
	
		 
	  if(q!=NULL)
	  {	
		q->lock = 1;
	  }
	  if (g!=NULL)
		g->lock=1;
	  if (p!=NULL){
		p->lock=1;
	  }	
	  
	  _xend();
	  count = 0;
	  count2 = 0;
	  
	}
		else{
		if (status & _XABORT_CONFLICT)
		{
			aborts[0]++;
			count++;
			count2++;
			if (count>9){
//				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);			
		}
		else if (status & _XABORT_CAPACITY){
			aborts[1]++;			
			count2++;
			if (count2<0)
			 return(-2);			
		}	
		else if (status & _XABORT_EXPLICIT)
		{
			aborts[2]++;
			if (_XABORT_CODE(status) == 0xfe)
				goto start_over;
			count++;
			count2++;
			if (count>9){
//				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);
		}
		else{
			aborts[3]++;
			count2++;
			if (count2<0)
			 return(-2);
		}
	}   
		
  }
}


int jsw_lookup2(struct jsw_tree *tree, int data){
  struct jsw_node * p;

  p = tree->root;

  while(1){
	if (data < p->data){
		if (p->link[0] == NULL) {return 0;}
			p = p->link[0]; 
	}
	else if (data > p->data){
		if (p->link[1] == NULL) {return 0;}
		p = p->link[1]; 
	}
	else{return 1;}				
  }
  
  return (-1);
}

int jsw_lookup(struct jsw_tree *tree, int data, long int * aborts,pthread_spinlock_t *lock){
  struct jsw_node * p;
  int count=0,count2=0;
  start_over:
  while(1){
	unsigned status = _xbegin();
	if (status == _XBEGIN_STARTED){
		if (((int)(*lock)) != 1) _xabort (0xfe);
		if (tree->root == NULL){
			_xend();
			return (0);
		}	
		if (tree->root->lock == 1) 
			_xabort(0xff);
		tree->root->lock = 1;
		p = tree->root;
		_xend();
		break;
	}
	else{
		if (status & _XABORT_CONFLICT)
		{
			aborts[0]++;
			count++;
			count2++;
			if (count>9){
				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);			
		}
		else if (status & _XABORT_CAPACITY){
			aborts[1]++;			
			count2++;
			if (count2<0)
			 return(-2);			
		}	
		else if (status & _XABORT_EXPLICIT)
		{
			aborts[2]++;
//			if (_XABORT_CODE(status) == 0xfe)
//				goto start_over;
			count++;
			count2++;
			if (count>9){
				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);
		}
		else{
			aborts[3]++;
			count2++;
			if (count2<0)
			 return(-2);
		}
	} 
  }
  count=0;
  count2=0;
  while(1){
	unsigned status = _xbegin();
	if (status == _XBEGIN_STARTED){
		if (((int)(*lock)) != 1) _xabort (0xfe);
		if (data < p->data){
			p->lock = 0; 
			if (p->link[0] == NULL) {_xend(); return 0;}
			if (p->link[0]->lock==1) 
				_xabort(0xff); 
			else
				p->link[0]->lock = 1;
			p = p->link[0]; 
			_xend();
			count=0;
			count2=0;
		}
		else if (data > p->data){
			p->lock = 0;
			if (p->link[1] == NULL) {_xend(); return 0;}
			if (p->link[1]->lock==1) 
				_xabort(0xff); 
			else
				p->link[1]->lock = 1;
				p = p->link[1]; 
			_xend();
			count=0;
			count2=0;
		}
		else{ p->lock=0; _xend(); return 1;}				
	}
	else{
		if (status & _XABORT_CONFLICT)
		{
			aborts[0]++;
			count++;
			count2++;
			if (count>9){
				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);			
		}
		else if (status & _XABORT_CAPACITY){
			aborts[1]++;			
			count2++;
			if (count2<0)
			 return(-2);			
		}	
		else if (status & _XABORT_EXPLICIT)
		{
			aborts[2]++;
//			if (_XABORT_CODE(status) == 0xfe)
//				goto start_over;
			count++;
			count2++;
			if (count>9){
				_mm_pause();
				//nanosleep(&tim , &tim2);
				count = 0;
			}
			if (count2<0)
			 return(-2);
		}
		else{
			aborts[3]++;
			count2++;
			if (count2<0)
			 return(-2);
		}
	}

  }
  
  return (-1);
}

int jsw_rb_assert ( struct jsw_node *root )
{
	int lh, rh;
	
	if ( root == NULL ) {
		return 1;
	} else {
		struct jsw_node *ln = root->link[0];
		struct jsw_node *rn = root->link[1];
		
		/* Consecutive red links */
		if ( is_red ( root ) ) {
			if ( is_red ( ln ) || is_red ( rn ) ) {
				puts ( "Red violation" );
				return 0;
			}
		}
		
		lh = jsw_rb_assert ( ln );
		rh = jsw_rb_assert ( rn );
		
		/* Invalid binary search tree */
		if ( ( ln != NULL && ln->data >= root->data )|| 
		     ( rn != NULL && rn->data <= root->data ) ) {
			puts ( "Binary tree violation" );
			return 0;
		}
		
		/* Black height mismatch */
		if ( lh != 0 && rh != 0 && lh != rh ) {
			puts ( "Black violation" );
			return 0;
		}
		
		/* Only count black links */
		if ( lh != 0 && rh != 0 )
			return is_red ( root ) ? lh : lh + 1;
		else
			return 0;
	}
}

int jsw_rb_assert1( struct jsw_tree *tree ){
	int result;
	result = jsw_rb_assert ( tree->root );
	return result;
}

void recursive_inorder_print(struct jsw_node * p){
  if (p!=NULL){
	recursive_inorder_print(p->link[0]);
	printf("[%d],[%d]\n",p->data,p->lock);
	recursive_inorder_print(p->link[1]);
  }
}

void jsw_print(struct jsw_tree * t){
  struct jsw_node * p = t->root;
  if (p==NULL)
    printf("[empty]\n");
  else
    recursive_inorder_print(p);
}

struct jsw_tree * jsw_create(void){
  struct jsw_tree * t;
  
  if ((t = malloc(sizeof(struct jsw_tree))) == NULL){
    perror("rb_create - malloc");
    exit(1);
  }

  pthread_spin_init(&t->spinlock, PTHREAD_PROCESS_SHARED);
  t->root = NULL;
  return t;
}

static inline int _rbt_warmup_helper(struct jsw_tree *rbt, int nr_nodes, 
                                     int max_key, unsigned int seed, int force)
{
	int i, nodes_inserted = 0, ret = 0;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;

		ret = jsw_insert2(rbt, key);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}

static int bh;
static int paths_with_bh_diff;
static int total_paths;
static int min_path_len, max_path_len;
static int total_nodes, red_nodes, black_nodes;
static int red_red_violations, bst_violations;
static void _rbt_validate_rec(struct jsw_node *root, int _bh, int _th)
{
	if (!root)
		return;

	struct jsw_node *left = root->link[0];
	struct jsw_node *right = root->link[1];

	total_nodes++;
	black_nodes += (IS_BLACK(root));
	red_nodes += (IS_RED(root));
	_th++;
	_bh += (IS_BLACK(root));

	/* BST violation? */
	if (left && left->data > root->data)
		bst_violations++;
	if (right && right->data <= root->data)
		bst_violations++;

	/* Red-Red violation? */
	if (IS_RED(root) && (IS_RED(left) || IS_RED(right)))
		red_red_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if (!left || !right) {
		total_paths++;
		if (bh == -1)
			bh = _bh;
		else if (_bh != bh)
			paths_with_bh_diff++;

		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* Check subtrees. */
	if (left)
		_rbt_validate_rec(left, _bh, _th);
	if (right)
		_rbt_validate_rec(right, _bh, _th);
}

static inline int _rbt_validate_helper(struct jsw_node *root)
{
	int check_bh = 0, check_red_red = 0, check_bst = 0;
	int check_rbt = 0;
	bh = -1;
	paths_with_bh_diff = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = black_nodes = red_nodes = 0;
	red_red_violations = 0;
	bst_violations = 0;

	_rbt_validate_rec(root, 0, 0);

	check_bh = (paths_with_bh_diff == 0);
	check_red_red = (red_red_violations == 0);
	check_bst = (bst_violations == 0);
	check_rbt = (check_bh && check_red_red && check_bst);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  Valid Red-Black Tree: %s\n",
	       check_rbt ? "Yes [OK]" : "No [ERROR]");
	printf("  Black height: %d [%s]\n", bh,
	       check_bh ? "OK" : "ERROR");
	printf("  Red-Red Violation: %s\n",
	       check_red_red ? "No [OK]" : "Yes [ERROR]");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size (Total / Black / Red): %8d / %8d / %8d\n",
	       total_nodes, black_nodes, red_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_rbt;
}

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
	printf("Size of tree node is %lu\n", sizeof(struct jsw_node));
	return jsw_create();
}

void *rbt_thread_data_new(int tid)
{
	return NULL;
}

void rbt_thread_data_print(void *thread_data)
{
	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
}

int rbt_lookup(void *rbt, void *thread_data, int key)
{
	int ret;
	long int aborts[4] = {0};

	ret = jsw_lookup(rbt, key, aborts, &((struct jsw_tree *)rbt)->spinlock);

	return ret;
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret = 0;
	long int aborts[4] = {0};

	ret = jsw_insert (rbt, key, aborts, &((struct jsw_tree *)rbt)->spinlock);

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret;
	long int aborts[4] = {0};

	ret = jsw_remove(rbt, key, aborts, &((struct jsw_tree *)rbt)->spinlock);

	return ret;
}

int rbt_validate(void *rbt)
{
	int ret;
	_rbt_validate_helper(((struct jsw_tree *)rbt)->root);
	ret = jsw_rb_assert(((struct jsw_tree *)rbt)->root);
	return ret;
}

int rbt_warmup(void *rbt, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _rbt_warmup_helper(rbt, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "mappouras_rbt";
}
