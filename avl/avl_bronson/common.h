/*   
 *   File: common.h
 *   Author: Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>, 
 *  	     Tudor David <tudor.david@epfl.ch>
 *   Description: 
 *   common.h is part of ASCYLIB
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

#ifndef _COMMON_H_
#define _COMMON_H_

#include <limits.h>
#include <string.h>

#define XSTR(s)                         STR(s)
#define STR(s)                          #s

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 6
#  define STATIC_ASSERT(a, msg)           _Static_assert ((a), msg);
#else 
#  define STATIC_ASSERT(a, msg)           
#endif

typedef int skey_t;
typedef uintptr_t sval_t;

#define KEY_MIN                         INT_MIN
#define KEY_MAX                         (INT_MAX - 2)


#if !defined(UNUSED)
#  define UNUSED __attribute__ ((unused))
#endif

#define likely(x)       __builtin_expect((x), 1)
#define unlikely(x)     __builtin_expect((x), 0)

#define MEM_BARRIER __sync_synchronize()

#endif	/*  _COMMON_H_ */
