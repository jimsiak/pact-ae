/*   
 *   File: lock_if.h
 *   Author: Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *   Description: 
 *   lock_if.h is part of ASCYLIB
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

#ifndef _LOCK_IF_H_
#define _LOCK_IF_H_

#define PREFETCHW_LOCK(lock)                            PREFETCHW(lock)

typedef pthread_spinlock_t ptlock_t;
#  define PTLOCK_SIZE sizeof(ptlock_t)
#  define INIT_LOCK(lock)				pthread_spin_init((pthread_spinlock_t *) lock, PTHREAD_PROCESS_PRIVATE);
#  define DESTROY_LOCK(lock)			        pthread_spin_destroy((pthread_spinlock_t *) lock)
#  define LOCK(lock)					pthread_spin_lock((pthread_spinlock_t *) lock)
#  define UNLOCK(lock)					pthread_spin_unlock((pthread_spinlock_t *) lock)
/* GLOBAL lock */
#  define GL_INIT_LOCK(lock)				pthread_spin_init((pthread_spinlock_t *) lock, PTHREAD_PROCESS_PRIVATE);
#  define GL_DESTROY_LOCK(lock)			        pthread_spin_destroy((pthread_spinlock_t *) lock)
#  define GL_LOCK(lock)					pthread_spin_lock((pthread_spinlock_t *) lock)
#  define GL_UNLOCK(lock)				pthread_spin_unlock((pthread_spinlock_t *) lock)

/* --------------------------------------------------------------------------------------------------- */
/* GLOBAL LOCK --------------------------------------------------------------------------------------- */
/* --------------------------------------------------------------------------------------------------- */


#if defined(LL_GLOBAL_LOCK)
#  define ND_GET_LOCK(nd)                 nd /* LOCK / UNLOCK are not defined in any case ;-) */

#  undef INIT_LOCK
#  undef DESTROY_LOCK
#  undef LOCK
#  undef UNLOCK
#  undef PREFETCHW_LOCK

#  define INIT_LOCK(lock)
#  define DESTROY_LOCK(lock)			
#  define LOCK(lock)
#  define UNLOCK(lock)
#  define PREFETCHW_LOCK(lock)

#  define INIT_LOCK_A(lock)       GL_INIT_LOCK(lock)
#  define DESTROY_LOCK_A(lock)    GL_DESTROY_LOCK(lock)			
#  define LOCK_A(lock)            GL_LOCK(lock)
#  define TRYLOCK_A(lock)         GL_TRYLOCK(lock)
#  define UNLOCK_A(lock)          GL_UNLOCK(lock)
#  define PREFETCHW_LOCK_A(lock)  

#else  /* !LL_GLOBAL_LOCK */
#  define ND_GET_LOCK(nd)                 &nd->lock

#  undef GL_INIT_LOCK
#  undef GL_DESTROY_LOCK
#  undef GL_LOCK
#  undef GL_UNLOCK

#  define GL_INIT_LOCK(lock)
#  define GL_DESTROY_LOCK(lock)			
#  define GL_LOCK(lock)
#  define GL_UNLOCK(lock)

#  define INIT_LOCK_A(lock)       INIT_LOCK(lock)
#  define DESTROY_LOCK_A(lock)    DESTROY_LOCK(lock)			
#  define LOCK_A(lock)            LOCK(lock)
#  define TRYLOCK_A(lock)         TRYLOCK(lock)
#  define UNLOCK_A(lock)          UNLOCK(lock)
#  define PREFETCHW_LOCK_A(lock)  PREFETCHW_LOCK(lock)

#endif

#endif	/* _LOCK_IF_H_ */
