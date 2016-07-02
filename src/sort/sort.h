/* Copyright (c) 2010-2014 Christopher Swenson. */
/* Copyright (c) 2012 Vojtech Fried. */
/* Copyright (c) 2012 Google Inc. All Rights Reserved. */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include "sort_common.h"

#ifndef SORT_NAME
#error "Must declare SORT_NAME"
#endif

#ifndef SORT_TYPE
#error "Must declare SORT_TYPE"
#endif

#ifndef SORT_CMP
#define SORT_CMP(x, y)  ((x) < (y) ? -1 : ((x) == (y) ? 0 : 1))
#endif

#ifndef TIM_SORT_STACK_SIZE
#define TIM_SORT_STACK_SIZE 128
#endif


#define SORT_SWAP(x,y) {SORT_TYPE __SORT_SWAP_t = (x); (x) = (y); (y) = __SORT_SWAP_t;}

#define SORT_CONCAT(x, y) x ## _ ## y
#define SORT_MAKE_STR1(x, y) SORT_CONCAT(x,y)
#define SORT_MAKE_STR(x) SORT_MAKE_STR1(SORT_NAME,x)

#define BINARY_INSERTION_FIND          SORT_MAKE_STR(binary_insertion_find)
#define BINARY_INSERTION_SORT_START    SORT_MAKE_STR(binary_insertion_sort_start)
#define BINARY_INSERTION_SORT          SORT_MAKE_STR(binary_insertion_sort)
#define REVERSE_ELEMENTS               SORT_MAKE_STR(reverse_elements)
#define COUNT_RUN                      SORT_MAKE_STR(count_run)
#define CHECK_INVARIANT                SORT_MAKE_STR(check_invariant)
#define TIM_SORT                       SORT_MAKE_STR(tim_sort)
#define TIM_SORT_RESIZE                SORT_MAKE_STR(tim_sort_resize)
#define TIM_SORT_MERGE                 SORT_MAKE_STR(tim_sort_merge)
#define TIM_SORT_COLLAPSE              SORT_MAKE_STR(tim_sort_collapse)
#define HEAP_SORT                      SORT_MAKE_STR(heap_sort)
#define MEDIAN                         SORT_MAKE_STR(median)
#define QUICK_SORT                     SORT_MAKE_STR(quick_sort)
#define MERGE_SORT                     SORT_MAKE_STR(merge_sort)
#define MERGE_SORT_IN_PLACE            SORT_MAKE_STR(merge_sort_in_place)
#define MERGE_SORT_IN_PLACE_RMERGE     SORT_MAKE_STR(merge_sort_in_place_rmerge)
#define MERGE_SORT_IN_PLACE_BACKMERGE  SORT_MAKE_STR(merge_sort_in_place_backmerge)
#define MERGE_SORT_IN_PLACE_FRONTMERGE SORT_MAKE_STR(merge_sort_in_place_frontmerge)
#define MERGE_SORT_IN_PLACE_ASWAP      SORT_MAKE_STR(merge_sort_in_place_aswap)
#define SELECTION_SORT                 SORT_MAKE_STR(selection_sort)
#define SHELL_SORT                     SORT_MAKE_STR(shell_sort)
#define QUICK_SORT_PARTITION           SORT_MAKE_STR(quick_sort_partition)
#define QUICK_SORT_RECURSIVE           SORT_MAKE_STR(quick_sort_recursive)
#define HEAP_SIFT_DOWN                 SORT_MAKE_STR(heap_sift_down)
#define HEAPIFY                        SORT_MAKE_STR(heapify)
#define TIM_SORT_RUN_T                 SORT_MAKE_STR(tim_sort_run_t)
#define TEMP_STORAGE_T                 SORT_MAKE_STR(temp_storage_t)
#define PUSH_NEXT                      SORT_MAKE_STR(push_next)
#define GRAIL_SWAP1                    SORT_MAKE_STR(grail_swap1)
#define REC_STABLE_SORT                SORT_MAKE_STR(rec_stable_sort)
#define GRAIL_REC_MERGE                SORT_MAKE_STR(grail_rec_merge)
#define GRAIL_SORT_DYN_BUFFER          SORT_MAKE_STR(grail_sort_dyn_buffer)
#define GRAIL_SORT_FIXED_BUFFER        SORT_MAKE_STR(grail_sort_fixed_buffer)
#define GRAIL_COMMON_SORT              SORT_MAKE_STR(grail_common_sort)
#define GRAIL_SORT                     SORT_MAKE_STR(grail_sort)
#define GRAIL_COMBINE_BLOCKS           SORT_MAKE_STR(grail_combine_blocks)
#define GRAIL_LAZY_STABLE_SORT         SORT_MAKE_STR(grail_lazy_stable_sort)
#define GRAIL_MERGE_WITHOUT_BUFFER     SORT_MAKE_STR(grail_merge_without_buffer)
#define GRAIL_ROTATE                   SORT_MAKE_STR(grail_rotate)
#define GRAIL_BIN_SEARCH_LEFT          SORT_MAKE_STR(grail_bin_search_left)
#define GRAIL_BUILD_BLOCKS             SORT_MAKE_STR(grail_build_blocks)
#define GRAIL_FIND_KEYS                SORT_MAKE_STR(grail_find_keys)
#define GRAIL_MERGE_BUFFERS_LEFT_WITH_X_BUF SORT_MAKE_STR(grail_merge_buffers_left_with_x_buf)
#define GRAIL_BIN_SEARCH_RIGHT         SORT_MAKE_STR(grail_bin_search_right)
#define GRAIL_MERGE_BUFFERS_LEFT       SORT_MAKE_STR(grail_merge_buffers_left)
#define GRAIL_SMART_MERGE_WITH_X_BUF   SORT_MAKE_STR(grail_smart_merge_with_x_buf)
#define GRAIL_MERGE_LEFT_WITH_X_BUF    SORT_MAKE_STR(grail_merge_left_with_x_buf)
#define GRAIL_SMART_MERGE_WITHOUT_BUFFER SORT_MAKE_STR(grail_smart_merge_without_buffer)
#define GRAIL_SMART_MERGE_WITH_BUFFER  SORT_MAKE_STR(grail_smart_merge_with_buffer)
#define GRAIL_MERGE_RIGHT              SORT_MAKE_STR(grail_merge_right)
#define GRAIL_MERGE_LEFT               SORT_MAKE_STR(grail_merge_left)
#define GRAIL_SWAP_N                   SORT_MAKE_STR(grail_swap_n)
#define SQRT_SORT                      SORT_MAKE_STR(sqrt_sort)
#define SQRT_SORT_BUILD_BLOCKS         SORT_MAKE_STR(sqrt_sort_build_blocks)
#define SQRT_SORT_MERGE_BUFFERS_LEFT_WITH_X_BUF SORT_MAKE_STR(sqrt_sort_merge_buffers_left_with_x_buf)
#define SQRT_SORT_MERGE_DOWN           SORT_MAKE_STR(sqrt_sort_merge_down)
#define SQRT_SORT_MERGE_LEFT_WITH_X_BUF SORT_MAKE_STR(sqrt_sort_merge_left_with_x_buf)
#define SQRT_SORT_MERGE_RIGHT          SORT_MAKE_STR(sqrt_sort_merge_right)
#define SQRT_SORT_SWAP_N               SORT_MAKE_STR(sqrt_sort_swap_n)
#define SQRT_SORT_SWAP_1               SORT_MAKE_STR(sqrt_sort_swap_1)
#define SQRT_SORT_SMART_MERGE_WITH_X_BUF SORT_MAKE_STR(sqrt_sort_smart_merge_with_x_buf)
#define SQRT_SORT_SORT_INS             SORT_MAKE_STR(sqrt_sort_sort_ins)
#define SQRT_SORT_COMBINE_BLOCKS       SORT_MAKE_STR(sqrt_sort_combine_blocks)
#define SQRT_SORT_COMMON_SORT          SORT_MAKE_STR(sqrt_sort_common_sort)
#define BUBBLE_SORT                    SORT_MAKE_STR(bubble_sort)

#ifndef MAX
#define MAX(x,y) (((x) > (y) ? (x) : (y)))
#endif
#ifndef MIN
#define MIN(x,y) (((x) < (y) ? (x) : (y)))
#endif

typedef struct {
  uint64_t start;
  uint64_t length;
} TIM_SORT_RUN_T;


void SHELL_SORT(SORT_TYPE *dst, const size_t size);
void BINARY_INSERTION_SORT(SORT_TYPE *dst, const size_t size);
void HEAP_SORT(SORT_TYPE *dst, const size_t size);
void QUICK_SORT(SORT_TYPE *dst, const size_t size);
void MERGE_SORT(SORT_TYPE *dst, const size_t size);
void MERGE_SORT_IN_PLACE(SORT_TYPE *dst, const size_t size);
void SELECTION_SORT(SORT_TYPE *dst, const size_t size);
void TIM_SORT(SORT_TYPE *dst, const size_t size);
void BUBBLE_SORT(SORT_TYPE *dst, const size_t size);


/* Shell sort implementation based on Wikipedia article
   http://en.wikipedia.org/wiki/Shell_sort
*/
void SHELL_SORT(SORT_TYPE *dst, const size_t size) {
  /* don't bother sorting an array of size 0 or 1 */
  if (size <= 1) {
    return;
  }

  /* TODO: binary search to find first gap? */
  int inci = 47;
  uint64_t inc = shell_gaps[inci];
  uint64_t i;

  while (inc > (size >> 1)) {
    inc = shell_gaps[--inci];
  }

  while (1) {
    for (i = inc; i < size; i++) {
      SORT_TYPE temp = dst[i];
      uint64_t j = i;

      while ((j >= inc) && (SORT_CMP(dst[j - inc], temp) > 0)) {
        dst[j] = dst[j - inc];
        j -= inc;
      }

      dst[j] = temp;
    }

    if (inc == 1) {
      break;
    }

    inc = shell_gaps[--inci];
  }
}

/* Function used to do a binary search for binary insertion sort */
static __inline int64_t BINARY_INSERTION_FIND(SORT_TYPE *dst, const SORT_TYPE x,
    const size_t size) {
  int64_t l, c, r;
  SORT_TYPE cx;
  l = 0;
  r = size - 1;
  c = r >> 1;

  /* check for out of bounds at the beginning. */
  if (SORT_CMP(x, dst[0]) < 0) {
    return 0;
  } else if (SORT_CMP(x, dst[r]) > 0) {
    return r;
  }

  cx = dst[c];

  while (1) {
    const int val = SORT_CMP(x, cx);

    if (val < 0) {
      if (c - l <= 1) {
        return c;
      }

      r = c;
    } else { /* allow = for stability. The binary search favors the right. */
      if (r - c <= 1) {
        return c + 1;
      }

      l = c;
    }

    c = l + ((r - l) >> 1);
    cx = dst[c];
  }
}

/* Binary insertion sort, but knowing that the first "start" entries are sorted.  Used in timsort. */
static void BINARY_INSERTION_SORT_START(SORT_TYPE *dst, const size_t start, const size_t size) {
  uint64_t i;

  for (i = start; i < size; i++) {
    int64_t j;
    SORT_TYPE x;
    int64_t location;

    /* If this entry is already correct, just move along */
    if (SORT_CMP(dst[i - 1], dst[i]) <= 0) {
      continue;
    }

    /* Else we need to find the right place, shift everything over, and squeeze in */
    x = dst[i];
    location = BINARY_INSERTION_FIND(dst, x, i);

    for (j = i - 1; j >= location; j--) {
      dst[j + 1] = dst[j];
    }

    dst[location] = x;
  }
}

/* Binary insertion sort */
void BINARY_INSERTION_SORT(SORT_TYPE *dst, const size_t size) {
  /* don't bother sorting an array of size <= 1 */
  if (size <= 1) {
    return;
  }

  BINARY_INSERTION_SORT_START(dst, 1, size);
}

/* Selection sort */
void SELECTION_SORT(SORT_TYPE *dst, const size_t size) {
  /* don't bother sorting an array of size <= 1 */
  if (size <= 1) {
    return;
  }

  uint64_t i;
  uint64_t j;

  for (i = 0; i < size; i++) {
    for (j = i + 1; j < size; j++) {
      if (SORT_CMP(dst[j], dst[i]) < 0) {
        SORT_SWAP(dst[i], dst[j]);
      }
    }
  }
}

/* In-place mergesort */
void MERGE_SORT_IN_PLACE_ASWAP(SORT_TYPE * dst1, SORT_TYPE * dst2, size_t len) {
  do {
    SORT_SWAP(*dst1, *dst2);
    dst1++;
    dst2++;
  } while (--len);
}

void MERGE_SORT_IN_PLACE_FRONTMERGE(SORT_TYPE *dst1, size_t l1, SORT_TYPE *dst2, size_t l2) {
  SORT_TYPE *dst0 = dst2 - l1;

  if (SORT_CMP(dst1[l1 - 1], dst2[0]) <= 0) {
    MERGE_SORT_IN_PLACE_ASWAP(dst1, dst0, l1);
    return;
  }

  do {
    while (SORT_CMP(*dst2, *dst1) > 0) {
      SORT_SWAP(*dst1, *dst0);
      dst1++;
      dst0++;

      if (--l1 == 0) {
        return;
      }
    }

    SORT_SWAP(*dst2, *dst0);
    dst2++;
    dst0++;
  } while (--l2);

  do {
    SORT_SWAP(*dst1, *dst0);
    dst1++;
    dst0++;
  } while (--l1);
}

size_t MERGE_SORT_IN_PLACE_BACKMERGE(SORT_TYPE * dst1, size_t l1, SORT_TYPE * dst2, size_t l2) {
  size_t res;
  SORT_TYPE *dst0 = dst2 + l1;

  if (SORT_CMP(dst1[1 - l1], dst2[0]) >= 0) {
    MERGE_SORT_IN_PLACE_ASWAP(dst1 - l1 + 1, dst0 - l1 + 1, l1);
    return l1;
  }

  do {
    while (SORT_CMP(*dst2, *dst1) < 0) {
      SORT_SWAP(*dst1, *dst0);
      dst1--;
      dst0--;

      if (--l1 == 0) {
        return 0;
      }
    }

    SORT_SWAP(*dst2, *dst0);
    dst2--;
    dst0--;
  } while (--l2);

  res = l1;

  do {
    SORT_SWAP(*dst1, *dst0);
    dst1--;
    dst0--;
  } while (--l1);

  return res;
}

/* merge dst[p0..p1) by buffer dst[p1..p1+r) */
void MERGE_SORT_IN_PLACE_RMERGE(SORT_TYPE *dst, size_t len, size_t lp, size_t r) {
  size_t i, lq;
  int cv;

  if (SORT_CMP(dst[lp], dst[lp - 1]) >= 0) {
    return;
  }

  lq = lp;

  for (i = 0; i < len; i += r) {
    /* select smallest dst[p0+n*r] */
    size_t q = i, j;

    for (j = lp; j <= lq; j += r) {
      cv = SORT_CMP(dst[j], dst[q]);

      if (cv == 0) {
        cv = SORT_CMP(dst[j + r - 1], dst[q + r - 1]);
      }

      if (cv < 0) {
        q = j;
      }
    }

    if (q != i) {
      MERGE_SORT_IN_PLACE_ASWAP(dst + i, dst + q, r); /* swap it with current position */

      if (q == lq && q < (len - r)) {
        lq += r;
      }
    }

    if (i != 0 && SORT_CMP(dst[i], dst[i - 1]) < 0) {
      MERGE_SORT_IN_PLACE_ASWAP(dst + len, dst + i, r); /* swap current position with buffer */
      MERGE_SORT_IN_PLACE_BACKMERGE(dst + (len + r - 1), r, dst + (i - 1),
                                    r);  /* buffer :merge: dst[i-r..i) -> dst[i-r..i+r) */
    }

    if (lp == i) {
      lp += r;
    }
  }
}

/* In-place Merge Sort implementation. (c)2012, Andrey Astrelin, astrelin@tochka.ru */
void MERGE_SORT_IN_PLACE(SORT_TYPE *dst, const size_t len) {
  /* don't bother sorting an array of size <= 1 */
  if (len <= 1) {
    return;
  }

  size_t r = rbnd(len);
  size_t lr = (len / r - 1) * r, p, m, q, q1, p0;
  SORT_TYPE *dst1 = dst - 1;

  if (len < 16) {
    BINARY_INSERTION_SORT(dst, len);
    return;
  }

  for (p = 2; p <= lr; p += 2) {
    dst1 += 2;

    if (SORT_CMP(dst1[0], dst1[-1]) < 0) {
      SORT_SWAP(dst1[0], dst1[-1]);
    }

    if (p & 2) {
      continue;
    }

    m = len - p;
    q = 2;

    while ((p & q) == 0) {
      if (SORT_CMP(dst1[1 - q], dst1[-q]) < 0) {
        break;
      }

      q *= 2;
    }

    if (p & q) {
      continue;
    }

    if (q < m) {
      p0 = len - q;
      MERGE_SORT_IN_PLACE_ASWAP(dst + p - q, dst + p0, q);

      for (;;) {
        q1 = 2 * q;

        if ((q1 > m) || (p & q1)) {
          break;
        }

        p0 = len - q1;
        MERGE_SORT_IN_PLACE_FRONTMERGE(dst + (p - q1), q, dst + p0 + q, q);
        q = q1;
      }

      MERGE_SORT_IN_PLACE_BACKMERGE(dst + (len - 1), q, dst1 - q, q);
      q *= 2;
    }

    q1 = q;

    while (q1 > m) {
      q1 /= 2;
    }

    while ((q & p) == 0) {
      q *= 2;
      MERGE_SORT_IN_PLACE_RMERGE(dst + (p - q), q, q / 2, q1);
    }
  }

  q1 = 0;

  for (q = r; q < lr; q *= 2) {
    if ((lr & q) != 0) {
      q1 += q;

      if (q1 != q) {
        MERGE_SORT_IN_PLACE_RMERGE(dst + (lr - q1), q1, q, r);
      }
    }
  }

  m = len - lr;
  MERGE_SORT_IN_PLACE(dst + lr, m);
  MERGE_SORT_IN_PLACE_ASWAP(dst, dst + lr, m);
  m += MERGE_SORT_IN_PLACE_BACKMERGE(dst + (m - 1), m, dst + (lr - 1), lr - m);
  MERGE_SORT_IN_PLACE(dst, m);
}

/* Standard merge sort */
void MERGE_SORT(SORT_TYPE *dst, const size_t size) {
  SORT_TYPE *newdst;
  const uint64_t middle = size / 2;
  uint64_t out = 0;
  uint64_t i = 0;
  uint64_t j = middle;

  /* don't bother sorting an array of size <= 1 */
  if (size <= 1) {
    return;
  }

  if (size < 16) {
    BINARY_INSERTION_SORT(dst, size);
    return;
  }

  MERGE_SORT(dst, middle);
  MERGE_SORT(&dst[middle], size - middle);
  newdst = (SORT_TYPE *) malloc(size * sizeof(SORT_TYPE));

  while (out != size) {
    if (i < middle) {
      if (j < size) {
        if (SORT_CMP(dst[i], dst[j]) <= 0) {
          newdst[out] = dst[i++];
        } else {
          newdst[out] = dst[j++];
        }
      } else {
        newdst[out] = dst[i++];
      }
    } else {
      newdst[out] = dst[j++];
    }

    out++;
  }

  memcpy(dst, newdst, size * sizeof(SORT_TYPE));
  free(newdst);
}


/* Quick sort: based on wikipedia */

static __inline size_t QUICK_SORT_PARTITION(SORT_TYPE *dst, const size_t left,
    const size_t right, const size_t pivot) {
  SORT_TYPE value = dst[pivot];
  size_t index = left;
  size_t i;
  int not_all_same = 0;
  /* move the pivot to the right */
  SORT_SWAP(dst[pivot], dst[right]);

  for (i = left; i < right; i++) {
    int cmp = SORT_CMP(dst[i], value);
    /* check if everything is all the same */
    not_all_same |= cmp;

    if (cmp < 0) {
      SORT_SWAP(dst[i], dst[index]);
      index++;
    }
  }

  SORT_SWAP(dst[right], dst[index]);

  /* avoid degenerate case */
  if (not_all_same == 0) {
    return SIZE_MAX;
  }

  return index;
}

/* Return the median index of the objects at the three indices. */
static __inline size_t MEDIAN(const SORT_TYPE *dst, const size_t a, const size_t b,
                              const size_t c) {
  const int AB = SORT_CMP(dst[a], dst[b]) < 0;

  if (AB) {
    /* a < b */
    const int BC = SORT_CMP(dst[b], dst[c]) < 0;

    if (BC) {
      /* a < b < c */
      return b;
    } else {
      /* a < b, c < b */
      const int AC = SORT_CMP(dst[a], dst[c]) < 0;

      if (AC) {
        /* a < c < b */
        return c;
      } else {
        /* c < a < b */
        return a;
      }
    }
  } else {
    /* b < a */
    const int AC = SORT_CMP(dst[a], dst[b]) < 0;

    if (AC) {
      /* b < a < c */
      return a;
    } else {
      /* b < a, c < a */
      const int BC = SORT_CMP(dst[b], dst[c]) < 0;

      if (BC) {
        /* b < c < a */
        return c;
      } else {
        /* c < b < a */
        return b;
      }
    }
  }
}

static void QUICK_SORT_RECURSIVE(SORT_TYPE *dst, const size_t left, const size_t right) {
  size_t pivot;
  size_t new_pivot;

  if (right <= left) {
    return;
  }

  if ((right - left + 1U) < 16U) {
    BINARY_INSERTION_SORT(&dst[left], right - left + 1U);
    return;
  }

  pivot = left + ((right - left) >> 1);
  /* this seems to perform worse by a small amount... ? */
  /* pivot = MEDIAN(dst, left, pivot, right); */
  new_pivot = QUICK_SORT_PARTITION(dst, left, right, pivot);

  /* check for partition all equal */
  if (new_pivot == SIZE_MAX) {
    return;
  }

  QUICK_SORT_RECURSIVE(dst, left, new_pivot - 1U);
  QUICK_SORT_RECURSIVE(dst, new_pivot + 1U, right);
}

void QUICK_SORT(SORT_TYPE *dst, const size_t size) {
  /* don't bother sorting an array of size 1 */
  if (size <= 1) {
    return;
  }

  QUICK_SORT_RECURSIVE(dst, 0U, size - 1U);
}


/* timsort implementation, based on timsort.txt */

static __inline void REVERSE_ELEMENTS(SORT_TYPE *dst, int64_t start, int64_t end) {
  while (1) {
    if (start >= end) {
      return;
    }

    SORT_SWAP(dst[start], dst[end]);
    start++;
    end--;
  }
}

static int64_t COUNT_RUN(SORT_TYPE *dst, const uint64_t start, const size_t size) {
  uint64_t curr;

  if (size - start == 1) {
    return 1;
  }

  if (start >= size - 2) {
    if (SORT_CMP(dst[size - 2], dst[size - 1]) > 0) {
      SORT_SWAP(dst[size - 2], dst[size - 1]);
    }

    return 2;
  }

  curr = start + 2;

  if (SORT_CMP(dst[start], dst[start + 1]) <= 0) {
    /* increasing run */
    while (1) {
      if (curr == size - 1) {
        break;
      }

      if (SORT_CMP(dst[curr - 1], dst[curr]) > 0) {
        break;
      }

      curr++;
    }

    return curr - start;
  } else {
    /* decreasing run */
    while (1) {
      if (curr == size - 1) {
        break;
      }

      if (SORT_CMP(dst[curr - 1], dst[curr]) <= 0) {
        break;
      }

      curr++;
    }

    /* reverse in-place */
    REVERSE_ELEMENTS(dst, start, curr - 1);
    return curr - start;
  }
}

static int CHECK_INVARIANT(TIM_SORT_RUN_T *stack, const int stack_curr) {
  int64_t A, B, C;

  if (stack_curr < 2) {
    return 1;
  }

  if (stack_curr == 2) {
    const int64_t A1 = stack[stack_curr - 2].length;
    const int64_t B1 = stack[stack_curr - 1].length;

    if (A1 <= B1) {
      return 0;
    }

    return 1;
  }

  A = stack[stack_curr - 3].length;
  B = stack[stack_curr - 2].length;
  C = stack[stack_curr - 1].length;

  if ((A <= B + C) || (B <= C)) {
    return 0;
  }

  return 1;
}

typedef struct {
  size_t alloc;
  SORT_TYPE *storage;
} TEMP_STORAGE_T;

static void TIM_SORT_RESIZE(TEMP_STORAGE_T *store, const size_t new_size) {
  if (store->alloc < new_size) {
    SORT_TYPE *tempstore = (SORT_TYPE *)realloc(store->storage, new_size * sizeof(SORT_TYPE));

    if (tempstore == NULL) {
      fprintf(stderr, "Error allocating temporary storage for tim sort: need %lu bytes",
              sizeof(SORT_TYPE) * new_size);
      exit(1);
    }

    store->storage = tempstore;
    store->alloc = new_size;
  }
}

static void TIM_SORT_MERGE(SORT_TYPE *dst, const TIM_SORT_RUN_T *stack, const int stack_curr,
                           TEMP_STORAGE_T *store) {
  const int64_t A = stack[stack_curr - 2].length;
  const int64_t B = stack[stack_curr - 1].length;
  const int64_t curr = stack[stack_curr - 2].start;
  SORT_TYPE *storage;
  int64_t i, j, k;
  TIM_SORT_RESIZE(store, MIN(A, B));
  storage = store->storage;

  /* left merge */
  if (A < B) {
    memcpy(storage, &dst[curr], A * sizeof(SORT_TYPE));
    i = 0;
    j = curr + A;

    for (k = curr; k < curr + A + B; k++) {
      if ((i < A) && (j < curr + A + B)) {
        if (SORT_CMP(storage[i], dst[j]) <= 0) {
          dst[k] = storage[i++];
        } else {
          dst[k] = dst[j++];
        }
      } else if (i < A) {
        dst[k] = storage[i++];
      } else {
        dst[k] = dst[j++];
      }
    }
  } else {
    /* right merge */
    memcpy(storage, &dst[curr + A], B * sizeof(SORT_TYPE));
    i = B - 1;
    j = curr + A - 1;

    for (k = curr + A + B - 1; k >= curr; k--) {
      if ((i >= 0) && (j >= curr)) {
        if (SORT_CMP(dst[j], storage[i]) > 0) {
          dst[k] = dst[j--];
        } else {
          dst[k] = storage[i--];
        }
      } else if (i >= 0) {
        dst[k] = storage[i--];
      } else {
        dst[k] = dst[j--];
      }
    }
  }
}

static int TIM_SORT_COLLAPSE(SORT_TYPE *dst, TIM_SORT_RUN_T *stack, int stack_curr,
                             TEMP_STORAGE_T *store, const size_t size) {
  while (1) {
    int64_t A, B, C, D;
    int ABC, BCD, CD;

    /* if the stack only has one thing on it, we are done with the collapse */
    if (stack_curr <= 1) {
      break;
    }

    /* if this is the last merge, just do it */
    if ((stack_curr == 2) && (stack[0].length + stack[1].length == size)) {
      TIM_SORT_MERGE(dst, stack, stack_curr, store);
      stack[0].length += stack[1].length;
      stack_curr--;
      break;
    }
    /* check if the invariant is off for a stack of 2 elements */
    else if ((stack_curr == 2) && (stack[0].length <= stack[1].length)) {
      TIM_SORT_MERGE(dst, stack, stack_curr, store);
      stack[0].length += stack[1].length;
      stack_curr--;
      break;
    } else if (stack_curr == 2) {
      break;
    }

    B = stack[stack_curr - 3].length;
    C = stack[stack_curr - 2].length;
    D = stack[stack_curr - 1].length;

    if (stack_curr >= 4) {
      A = stack[stack_curr - 4].length;
      ABC = (A <= B + C);
    } else {
      ABC = 0;
    }

    BCD = (B <= C + D) || ABC;
    CD = (C <= D);

    /* Both invariants are good */
    if (!BCD && !CD) {
      break;
    }

    /* left merge */
    if (BCD && !CD) {
      TIM_SORT_MERGE(dst, stack, stack_curr - 1, store);
      stack[stack_curr - 3].length += stack[stack_curr - 2].length;
      stack[stack_curr - 2] = stack[stack_curr - 1];
      stack_curr--;
    } else {
      /* right merge */
      TIM_SORT_MERGE(dst, stack, stack_curr, store);
      stack[stack_curr - 2].length += stack[stack_curr - 1].length;
      stack_curr--;
    }
  }

  return stack_curr;
}

static __inline int PUSH_NEXT(SORT_TYPE *dst,
                              const size_t size,
                              TEMP_STORAGE_T *store,
                              const uint64_t minrun,
                              TIM_SORT_RUN_T *run_stack,
                              uint64_t *stack_curr,
                              uint64_t *curr) {
  uint64_t len = COUNT_RUN(dst, *curr, size);
  uint64_t run = minrun;

  if (run > size - *curr) {
    run = size - *curr;
  }

  if (run > len) {
    BINARY_INSERTION_SORT_START(&dst[*curr], len, run);
    len = run;
  }

  run_stack[*stack_curr].start = *curr;
  run_stack[*stack_curr].length = len;
  (*stack_curr)++;
  *curr += len;

  if (*curr == size) {
    /* finish up */
    while (*stack_curr > 1) {
      TIM_SORT_MERGE(dst, run_stack, *stack_curr, store);
      run_stack[*stack_curr - 2].length += run_stack[*stack_curr - 1].length;
      (*stack_curr)--;
    }

    if (store->storage != NULL) {
      free(store->storage);
      store->storage = NULL;
    }

    return 0;
  }

  return 1;
}

void TIM_SORT(SORT_TYPE *dst, const size_t size) {
  /* don't bother sorting an array of size 1 */
  if (size <= 1) {
    return;
  }

  uint64_t minrun;
  TEMP_STORAGE_T _store, *store;
  TIM_SORT_RUN_T run_stack[TIM_SORT_STACK_SIZE];
  uint64_t stack_curr = 0;
  uint64_t curr = 0;

  if (size < 64) {
    BINARY_INSERTION_SORT(dst, size);
    return;
  }

  /* compute the minimum run length */
  minrun = compute_minrun(size);
  /* temporary storage for merges */
  store = &_store;
  store->alloc = 0;
  store->storage = NULL;

  if (!PUSH_NEXT(dst, size, store, minrun, run_stack, &stack_curr, &curr)) {
    return;
  }

  if (!PUSH_NEXT(dst, size, store, minrun, run_stack, &stack_curr, &curr)) {
    return;
  }

  if (!PUSH_NEXT(dst, size, store, minrun, run_stack, &stack_curr, &curr)) {
    return;
  }

  while (1) {
    if (!CHECK_INVARIANT(run_stack, stack_curr)) {
      stack_curr = TIM_SORT_COLLAPSE(dst, run_stack, stack_curr, store, size);
      continue;
    }

    if (!PUSH_NEXT(dst, size, store, minrun, run_stack, &stack_curr, &curr)) {
      return;
    }
  }
}

/* heap sort: based on wikipedia */

static __inline void HEAP_SIFT_DOWN(SORT_TYPE *dst, const int64_t start, const int64_t end) {
  int64_t root = start;

  while ((root << 1) <= end) {
    int64_t child = root << 1;

    if ((child < end) && (SORT_CMP(dst[child], dst[child + 1]) < 0)) {
      child++;
    }

    if (SORT_CMP(dst[root], dst[child]) < 0) {
      SORT_SWAP(dst[root], dst[child]);
      root = child;
    } else {
      return;
    }
  }
}

static __inline void HEAPIFY(SORT_TYPE *dst, const size_t size) {
  int64_t start = size >> 1;

  while (start >= 0) {
    HEAP_SIFT_DOWN(dst, start, size - 1);
    start--;
  }
}

void HEAP_SORT(SORT_TYPE *dst, const size_t size) {
  /* don't bother sorting an array of size <= 1 */
  if (size <= 1) {
    return;
  }

  int64_t end = size - 1;
  HEAPIFY(dst, size);

  while (end > 0) {
    SORT_SWAP(dst[end], dst[0]);
    HEAP_SIFT_DOWN(dst, 0, end - 1);
    end--;
  }
}

/********* Sqrt sorting *********************************/
/*                                                       */
/* (c) 2014 by Andrey Astrelin                           */
/*                                                       */
/*                                                       */
/* Stable sorting that works in O(N*log(N)) worst time   */
/* and uses O(sqrt(N)) extra memory                      */
/*                                                       */
/* Define SORT_TYPE and SORT_CMP                         */
/* and then call SqrtSort() function                     */
/*                                                       */
/*********************************************************/

#define SORT_CMP_A(a,b) SORT_CMP(*(a),*(b))

static __inline void SQRT_SORT_SWAP_1(SORT_TYPE *a, SORT_TYPE *b) {
  SORT_TYPE c = *a;
  *a++ = *b;
  *b++ = c;
}

static __inline void SQRT_SORT_SWAP_N(SORT_TYPE *a, SORT_TYPE *b, int n) {
  while (n--) {
    SQRT_SORT_SWAP_1(a++, b++);
  }
}


static void SQRT_SORT_MERGE_RIGHT(SORT_TYPE *arr, int L1, int L2, int M) {
  int p0 = L1 + L2 + M - 1, p2 = L1 + L2 - 1, p1 = L1 - 1;

  while (p1 >= 0) {
    if (p2 < L1 || SORT_CMP_A(arr + p1, arr + p2) > 0) {
      arr[p0--] = arr[p1--];
    } else {
      arr[p0--] = arr[p2--];
    }
  }

  if (p2 != p0) while (p2 >= L1) {
      arr[p0--] = arr[p2--];
    }
}

/* arr[M..-1] - free, arr[0,L1-1]++arr[L1,L1+L2-1] -> arr[M,M+L1+L2-1] */
static void SQRT_SORT_MERGE_LEFT_WITH_X_BUF(SORT_TYPE *arr, int L1, int L2, int M) {
  int p0 = 0, p1 = L1;
  L2 += L1;

  while (p1 < L2) {
    if (p0 == L1 || SORT_CMP_A(arr + p0, arr + p1) > 0) {
      arr[M++] = arr[p1++];
    } else {
      arr[M++] = arr[p0++];
    }
  }

  if (M != p0) while (p0 < L1) {
      arr[M++] = arr[p0++];
    }
}

/* arr[0,L1-1] ++ arr2[0,L2-1] -> arr[-L1,L2-1],  arr2 is "before" arr1 */
static void SQRT_SORT_MERGE_DOWN(SORT_TYPE *arr, SORT_TYPE *arr2, int L1, int L2) {
  int p0 = 0, p1 = 0, M = -L2;

  while (p1 < L2) {
    if (p0 == L1 || SORT_CMP_A(arr + p0, arr2 + p1) >= 0) {
      arr[M++] = arr2[p1++];
    } else {
      arr[M++] = arr[p0++];
    }
  }

  if (M != p0) while (p0 < L1) {
      arr[M++] = arr[p0++];
    }
}

static void SQRT_SORT_SMART_MERGE_WITH_X_BUF(SORT_TYPE *arr, int *alen1, int *atype, int len2,
    int lkeys) {
  int p0 = -lkeys, p1 = 0, p2 = *alen1, q1 = p2, q2 = p2 + len2;
  int ftype = 1 - *atype; /* 1 if inverted */

  while (p1 < q1 && p2 < q2) {
    if (SORT_CMP_A(arr + p1, arr + p2) - ftype < 0) {
      arr[p0++] = arr[p1++];
    } else {
      arr[p0++] = arr[p2++];
    }
  }

  if (p1 < q1) {
    *alen1 = q1 - p1;

    while (p1 < q1) {
      arr[--q2] = arr[--q1];
    }
  } else {
    *alen1 = q2 - p2;
    *atype = ftype;
  }
}


/*
  arr - starting array. arr[-lblock..-1] - buffer (if havebuf).
  lblock - length of regular blocks. First nblocks are stable sorted by 1st elements and key-coded
  keys - arrays of keys, in same order as blocks. key<midkey means stream A
  nblock2 are regular blocks from stream A. llast is length of last (irregular) block from stream B, that should go before nblock2 blocks.
  llast=0 requires nblock2=0 (no irregular blocks). llast>0, nblock2=0 is possible.
*/
static void SQRT_SORT_MERGE_BUFFERS_LEFT_WITH_X_BUF(int *keys, int midkey, SORT_TYPE *arr,
    int nblock, int lblock, int nblock2, int llast) {
  int l, prest, lrest, frest, pidx, cidx, fnext;

  if (nblock == 0) {
    l = nblock2 * lblock;
    SQRT_SORT_MERGE_LEFT_WITH_X_BUF(arr, l, llast, -lblock);
    return;
  }

  lrest = lblock;
  frest = keys[0] < midkey ? 0 : 1;
  pidx = lblock;

  for (cidx = 1; cidx < nblock; cidx++, pidx += lblock) {
    prest = pidx - lrest;
    fnext = keys[cidx] < midkey ? 0 : 1;

    if (fnext == frest) {
      memcpy(arr + prest - lblock, arr + prest, lrest * sizeof(SORT_TYPE));
      prest = pidx;
      lrest = lblock;
    } else {
      SQRT_SORT_SMART_MERGE_WITH_X_BUF(arr + prest, &lrest, &frest, lblock, lblock);
    }
  }

  prest = pidx - lrest;

  if (llast) {
    if (frest) {
      memcpy(arr + prest - lblock, arr + prest, lrest * sizeof(SORT_TYPE));
      prest = pidx;
      lrest = lblock * nblock2;
      frest = 0;
    } else {
      lrest += lblock * nblock2;
    }

    SQRT_SORT_MERGE_LEFT_WITH_X_BUF(arr + prest, lrest, llast, -lblock);
  } else {
    memcpy(arr + prest - lblock, arr + prest, lrest * sizeof(SORT_TYPE));
  }
}

/*
  build blocks of length K
  input: [-K,-1] elements are buffer
  output: first K elements are buffer, blocks 2*K and last subblock sorted
*/
static void SQRT_SORT_BUILD_BLOCKS(SORT_TYPE *arr, int L, int K) {
  int m, u, h, p0, p1, rest, restk, p;

  for (m = 1; m < L; m += 2) {
    u = 0;

    if (SORT_CMP_A(arr + (m - 1), arr + m) > 0) {
      u = 1;
    }

    arr[m - 3] = arr[m - 1 + u];
    arr[m - 2] = arr[m - u];
  }

  if (L % 2) {
    arr[L - 3] = arr[L - 1];
  }

  arr -= 2;

  for (h = 2; h < K; h *= 2) {
    p0 = 0;
    p1 = L - 2 * h;

    while (p0 <= p1) {
      SQRT_SORT_MERGE_LEFT_WITH_X_BUF(arr + p0, h, h, -h);
      p0 += 2 * h;
    }

    rest = L - p0;

    if (rest > h) {
      SQRT_SORT_MERGE_LEFT_WITH_X_BUF(arr + p0, h, rest - h, -h);
    } else {
      for (; p0 < L; p0++) {
        arr[p0 - h] = arr[p0];
      }
    }

    arr -= h;
  }

  restk = L % (2 * K);
  p = L - restk;

  if (restk <= K) {
    memcpy(arr + p + K, arr + p, restk * sizeof(SORT_TYPE));
  } else {
    SQRT_SORT_MERGE_RIGHT(arr + p, K, restk - K, K);
  }

  while (p > 0) {
    p -= 2 * K;
    SQRT_SORT_MERGE_RIGHT(arr + p, K, K, K);
  }
}


static void SQRT_SORT_SORT_INS(SORT_TYPE *arr, int len) {
  int i, j;

  for (i = 1; i < len; i++) {
    for (j = i - 1; j >= 0 && SORT_CMP_A(arr + (j + 1), arr + j) < 0; j--) {
      SQRT_SORT_SWAP_1(arr + j, arr + (j + 1));
    }
  }
}

/*
  keys are on the left of arr. Blocks of length LL combined. We'll combine them in pairs
  LL and nkeys are powers of 2. (2*LL/lblock) keys are guarantied
*/
static void SQRT_SORT_COMBINE_BLOCKS(SORT_TYPE *arr, int len, int LL, int lblock, int *tags) {
  int M, b, NBlk, midkey, lrest, u, i, p, v, kc, nbl2, llast;
  SORT_TYPE *arr1;
  M = len / (2 * LL);
  lrest = len % (2 * LL);

  if (lrest <= LL) {
    len -= lrest;
    lrest = 0;
  }

  for (b = 0; b <= M; b++) {
    if (b == M && lrest == 0) {
      break;
    }

    arr1 = arr + b * 2 * LL;
    NBlk = (b == M ? lrest : 2 * LL) / lblock;
    u = NBlk + (b == M ? 1 : 0);

    for (i = 0; i <= u; i++) {
      tags[i] = i;
    }

    midkey = LL / lblock;

    for (u = 1; u < NBlk; u++) {
      p = u - 1;

      for (v = u; v < NBlk; v++) {
        kc = SORT_CMP_A(arr1 + p * lblock, arr1 + v * lblock);

        if (kc > 0 || (kc == 0 && tags[p] > tags[v])) {
          p = v;
        }
      }

      if (p != u - 1) {
        SQRT_SORT_SWAP_N(arr1 + (u - 1)*lblock, arr1 + p * lblock, lblock);
        i = tags[u - 1];
        tags[u - 1] = tags[p];
        tags[p] = i;
      }
    }

    nbl2 = llast = 0;

    if (b == M) {
      llast = lrest % lblock;
    }

    if (llast != 0) {
      while (nbl2 < NBlk && SORT_CMP_A(arr1 + NBlk * lblock, arr1 + (NBlk - nbl2 - 1)*lblock) < 0) {
        nbl2++;
      }
    }

    SQRT_SORT_MERGE_BUFFERS_LEFT_WITH_X_BUF(tags, midkey, arr1, NBlk - nbl2, lblock, nbl2, llast);
  }

  for (p = len; --p >= 0;) {
    arr[p] = arr[p - lblock];
  }
}


static void SQRT_SORT_COMMON_SORT(SORT_TYPE *arr, int Len, SORT_TYPE *extbuf, int *Tags) {
  int lblock, cbuf;

  if (Len < 16) {
    SQRT_SORT_SORT_INS(arr, Len);
    return;
  }

  lblock = 1;

  while (lblock * lblock < Len) {
    lblock *= 2;
  }

  memcpy(extbuf, arr, lblock * sizeof(SORT_TYPE));
  SQRT_SORT_COMMON_SORT(extbuf, lblock, arr, Tags);
  SQRT_SORT_BUILD_BLOCKS(arr + lblock, Len - lblock, lblock);
  cbuf = lblock;

  while (Len > (cbuf *= 2)) {
    SQRT_SORT_COMBINE_BLOCKS(arr + lblock, Len - lblock, cbuf, lblock, Tags);
  }

  SQRT_SORT_MERGE_DOWN(arr + lblock, extbuf, Len - lblock, lblock);
}

static void SQRT_SORT(SORT_TYPE *arr, size_t Len) {
  int L = 1;
  SORT_TYPE *ExtBuf;
  int *Tags;

  while (L * L < Len) {
    L *= 2;
  }

  int NK = (Len - 1) / L + 2;
  ExtBuf = (SORT_TYPE*)malloc(L * sizeof(SORT_TYPE));

  if (ExtBuf == NULL) {
    return;  /* fail */
  }

  Tags = (int*)malloc(NK * sizeof(int));

  if (Tags == NULL) {
    return;
  }

  SQRT_SORT_COMMON_SORT(arr, Len, ExtBuf, Tags);
  free(Tags);
  free(ExtBuf);
}

/********* Grail sorting *********************************/
/*                                                       */
/* (c) 2013 by Andrey Astrelin                           */
/*                                                       */
/*                                                       */
/* Stable sorting that works in O(N*log(N)) worst time   */
/* and uses O(1) extra memory                            */
/*                                                       */
/* Define SORT_TYPE and SORT_CMP                         */
/* and then call GrailSort() function                    */
/*                                                       */
/* For sorting with fixed external buffer (512 items)    */
/* use GrailSortWithBuffer()                             */
/*                                                       */
/* For sorting with dynamic external buffer (O(sqrt(N)) items) */
/* use GrailSortWithDynBuffer()                          */
/*                                                       */
/* Also classic in-place merge sort is implemented       */
/* under the name of RecStableSort()                     */
/*                                                       */
/*********************************************************/

#define GRAIL_EXT_BUFFER_LENGTH 512

static __inline void GRAIL_SWAP1(SORT_TYPE *a, SORT_TYPE *b) {
  SORT_TYPE c = *a;
  *a = *b;
  *b = c;
}

static __inline void GRAIL_SWAP_N(SORT_TYPE *a, SORT_TYPE *b, int n) {
  while (n--) {
    GRAIL_SWAP1(a++, b++);
  }
}

static void GRAIL_ROTATE(SORT_TYPE *a, int l1, int l2) {
  while (l1 && l2) {
    if (l1 <= l2) {
      GRAIL_SWAP_N(a, a + l1, l1);
      a += l1;
      l2 -= l1;
    } else {
      GRAIL_SWAP_N(a + (l1 - l2), a + l1, l2);
      l1 -= l2;
    }
  }
}

static int GRAIL_BIN_SEARCH_LEFT(SORT_TYPE *arr, int len, SORT_TYPE *key) {
  int a = -1, b = len, c;

  while (a < b - 1) {
    c = a + ((b - a) >> 1);

    if (SORT_CMP_A(arr + c, key) >= 0) {
      b = c;
    } else {
      a = c;
    }
  }

  return b;
}
static int GRAIL_BIN_SEARCH_RIGHT(SORT_TYPE *arr, int len, SORT_TYPE *key) {
  int a = -1, b = len, c;

  while (a < b - 1) {
    c = a + ((b - a) >> 1);

    if (SORT_CMP_A(arr + c, key) > 0) {
      b = c;
    } else {
      a = c;
    }
  }

  return b;
}

/* cost: 2*len+nk^2/2 */
static int GRAIL_FIND_KEYS(SORT_TYPE *arr, int len, int nkeys) {
  int h = 1, h0 = 0; /* first key is always here */
  int u = 1, r;

  while (u < len && h < nkeys) {
    r = GRAIL_BIN_SEARCH_LEFT(arr + h0, h, arr + u);

    if (r == h || SORT_CMP_A(arr + u, arr + (h0 + r)) != 0) {
      GRAIL_ROTATE(arr + h0, h, u - (h0 + h));
      h0 = u - h;
      GRAIL_ROTATE(arr + (h0 + r), h - r, 1);
      h++;
    }

    u++;
  }

  GRAIL_ROTATE(arr, h0, h);
  return h;
}

/* cost: min(L1,L2)^2+max(L1,L2) */
static void GRAIL_MERGE_WITHOUT_BUFFER(SORT_TYPE *arr, int len1, int len2) {
  int h;

  if (len1 < len2) {
    while (len1) {
      h = GRAIL_BIN_SEARCH_LEFT(arr + len1, len2, arr);

      if (h != 0) {
        GRAIL_ROTATE(arr, len1, h);
        arr += h;
        len2 -= h;
      }

      if (len2 == 0) {
        break;
      }

      do {
        arr++;
        len1--;
      } while (len1 && SORT_CMP_A(arr, arr + len1) <= 0);
    }
  } else {
    while (len2) {
      h = GRAIL_BIN_SEARCH_RIGHT(arr, len1, arr + (len1 + len2 - 1));

      if (h != len1) {
        GRAIL_ROTATE(arr + h, len1 - h, len2);
        len1 = h;
      }

      if (len1 == 0) {
        break;
      }

      do {
        len2--;
      } while (len2 && SORT_CMP_A(arr + len1 - 1, arr + len1 + len2 - 1) <= 0);
    }
  }
}

/* arr[M..-1] - buffer, arr[0,L1-1]++arr[L1,L1+L2-1] -> arr[M,M+L1+L2-1] */
static void GRAIL_MERGE_LEFT(SORT_TYPE *arr, int L1, int L2, int M) {
  int p0 = 0, p1 = L1;
  L2 += L1;

  while (p1 < L2) {
    if (p0 == L1 || SORT_CMP_A(arr + p0, arr + p1) > 0) {
      GRAIL_SWAP1(arr + (M++), arr + (p1++));
    } else {
      GRAIL_SWAP1(arr + (M++), arr + (p0++));
    }
  }

  if (M != p0) {
    GRAIL_SWAP_N(arr + M, arr + p0, L1 - p0);
  }
}
static void GRAIL_MERGE_RIGHT(SORT_TYPE *arr, int L1, int L2, int M) {
  int p0 = L1 + L2 + M - 1, p2 = L1 + L2 - 1, p1 = L1 - 1;

  while (p1 >= 0) {
    if (p2 < L1 || SORT_CMP_A(arr + p1, arr + p2) > 0) {
      GRAIL_SWAP1(arr + (p0--), arr + (p1--));
    } else {
      GRAIL_SWAP1(arr + (p0--), arr + (p2--));
    }
  }

  if (p2 != p0) while (p2 >= L1) {
      GRAIL_SWAP1(arr + (p0--), arr + (p2--));
    }
}

static void GRAIL_SMART_MERGE_WITH_BUFFER(SORT_TYPE *arr, int *alen1, int *atype, int len2,
    int lkeys) {
  int p0 = -lkeys, p1 = 0, p2 = *alen1, q1 = p2, q2 = p2 + len2;
  int ftype = 1 - *atype; /* 1 if inverted */

  while (p1 < q1 && p2 < q2) {
    if (SORT_CMP_A(arr + p1, arr + p2) - ftype < 0) {
      GRAIL_SWAP1(arr + (p0++), arr + (p1++));
    } else {
      GRAIL_SWAP1(arr + (p0++), arr + (p2++));
    }
  }

  if (p1 < q1) {
    *alen1 = q1 - p1;

    while (p1 < q1) {
      GRAIL_SWAP1(arr + (--q1), arr + (--q2));
    }
  } else {
    *alen1 = q2 - p2;
    *atype = ftype;
  }
}
static void GRAIL_SMART_MERGE_WITHOUT_BUFFER(SORT_TYPE *arr, int *alen1, int *atype, int _len2) {
  int len1, len2, ftype, h;

  if (!_len2) {
    return;
  }

  len1 = *alen1;
  len2 = _len2;
  ftype = 1 - *atype;

  if (len1 && SORT_CMP_A(arr + (len1 - 1), arr + len1) - ftype >= 0) {
    while (len1) {
      h = ftype ? GRAIL_BIN_SEARCH_LEFT(arr + len1, len2, arr) : GRAIL_BIN_SEARCH_RIGHT(arr + len1, len2,
          arr);

      if (h != 0) {
        GRAIL_ROTATE(arr, len1, h);
        arr += h;
        len2 -= h;
      }

      if (len2 == 0) {
        *alen1 = len1;
        return;
      }

      do {
        arr++;
        len1--;
      } while (len1 && SORT_CMP_A(arr, arr + len1) - ftype < 0);
    }
  }

  *alen1 = len2;
  *atype = ftype;
}

/***** Sort With Extra Buffer *****/

/* arr[M..-1] - free, arr[0,L1-1]++arr[L1,L1+L2-1] -> arr[M,M+L1+L2-1] */
static void GRAIL_MERGE_LEFT_WITH_X_BUF(SORT_TYPE *arr, int L1, int L2, int M) {
  int p0 = 0, p1 = L1;
  L2 += L1;

  while (p1 < L2) {
    if (p0 == L1 || SORT_CMP_A(arr + p0, arr + p1) > 0) {
      arr[M++] = arr[p1++];
    } else {
      arr[M++] = arr[p0++];
    }
  }

  if (M != p0) while (p0 < L1) {
      arr[M++] = arr[p0++];
    }
}

static void GRAIL_SMART_MERGE_WITH_X_BUF(SORT_TYPE *arr, int *alen1, int *atype, int len2,
    int lkeys) {
  int p0 = -lkeys, p1 = 0, p2 = *alen1, q1 = p2, q2 = p2 + len2;
  int ftype = 1 - *atype; /* 1 if inverted */

  while (p1 < q1 && p2 < q2) {
    if (SORT_CMP_A(arr + p1, arr + p2) - ftype < 0) {
      arr[p0++] = arr[p1++];
    } else {
      arr[p0++] = arr[p2++];
    }
  }

  if (p1 < q1) {
    *alen1 = q1 - p1;

    while (p1 < q1) {
      arr[--q2] = arr[--q1];
    }
  } else {
    *alen1 = q2 - p2;
    *atype = ftype;
  }
}

/*
  arr - starting array. arr[-lblock..-1] - buffer (if havebuf).
  lblock - length of regular blocks. First nblocks are stable sorted by 1st elements and key-coded
  keys - arrays of keys, in same order as blocks. key<midkey means stream A
  nblock2 are regular blocks from stream A. llast is length of last (irregular) block from stream B, that should go before nblock2 blocks.
  llast=0 requires nblock2=0 (no irregular blocks). llast>0, nblock2=0 is possible.
*/
static void GRAIL_MERGE_BUFFERS_LEFT_WITH_X_BUF(SORT_TYPE *keys, SORT_TYPE *midkey, SORT_TYPE *arr,
    int nblock, int lblock, int nblock2, int llast) {
  int l, prest, lrest, frest, pidx, cidx, fnext;

  if (nblock == 0) {
    l = nblock2 * lblock;
    GRAIL_MERGE_LEFT_WITH_X_BUF(arr, l, llast, -lblock);
    return;
  }

  lrest = lblock;
  frest = SORT_CMP_A(keys, midkey) < 0 ? 0 : 1;
  pidx = lblock;

  for (cidx = 1; cidx < nblock; cidx++, pidx += lblock) {
    prest = pidx - lrest;
    fnext = SORT_CMP_A(keys + cidx, midkey) < 0 ? 0 : 1;

    if (fnext == frest) {
      memcpy(arr + prest - lblock, arr + prest, lrest * sizeof(SORT_TYPE));
      prest = pidx;
      lrest = lblock;
    } else {
      GRAIL_SMART_MERGE_WITH_X_BUF(arr + prest, &lrest, &frest, lblock, lblock);
    }
  }

  prest = pidx - lrest;

  if (llast) {
    if (frest) {
      memcpy(arr + prest - lblock, arr + prest, lrest * sizeof(SORT_TYPE));
      prest = pidx;
      lrest = lblock * nblock2;
      frest = 0;
    } else {
      lrest += lblock * nblock2;
    }

    GRAIL_MERGE_LEFT_WITH_X_BUF(arr + prest, lrest, llast, -lblock);
  } else {
    memcpy(arr + prest - lblock, arr + prest, lrest * sizeof(SORT_TYPE));
  }
}

/***** End Sort With Extra Buffer *****/

/*
  build blocks of length K
  input: [-K,-1] elements are buffer
  output: first K elements are buffer, blocks 2*K and last subblock sorted
*/
static void GRAIL_BUILD_BLOCKS(SORT_TYPE *arr, int L, int K, SORT_TYPE *extbuf, int LExtBuf) {
  int m, u, h, p0, p1, rest, restk, p, kbuf;
  kbuf = K < LExtBuf ? K : LExtBuf;

  while (kbuf & (kbuf - 1)) {
    kbuf &= kbuf - 1;  /* max power or 2 - just in case */
  }

  if (kbuf) {
    memcpy(extbuf, arr - kbuf, kbuf * sizeof(SORT_TYPE));

    for (m = 1; m < L; m += 2) {
      u = 0;

      if (SORT_CMP_A(arr + (m - 1), arr + m) > 0) {
        u = 1;
      }

      arr[m - 3] = arr[m - 1 + u];
      arr[m - 2] = arr[m - u];
    }

    if (L % 2) {
      arr[L - 3] = arr[L - 1];
    }

    arr -= 2;

    for (h = 2; h < kbuf; h *= 2) {
      p0 = 0;
      p1 = L - 2 * h;

      while (p0 <= p1) {
        GRAIL_MERGE_LEFT_WITH_X_BUF(arr + p0, h, h, -h);
        p0 += 2 * h;
      }

      rest = L - p0;

      if (rest > h) {
        GRAIL_MERGE_LEFT_WITH_X_BUF(arr + p0, h, rest - h, -h);
      } else {
        for (; p0 < L; p0++) {
          arr[p0 - h] = arr[p0];
        }
      }

      arr -= h;
    }

    memcpy(arr + L, extbuf, kbuf * sizeof(SORT_TYPE));
  } else {
    for (m = 1; m < L; m += 2) {
      u = 0;

      if (SORT_CMP_A(arr + (m - 1), arr + m) > 0) {
        u = 1;
      }

      GRAIL_SWAP1(arr + (m - 3), arr + (m - 1 + u));
      GRAIL_SWAP1(arr + (m - 2), arr + (m - u));
    }

    if (L % 2) {
      GRAIL_SWAP1(arr + (L - 1), arr + (L - 3));
    }

    arr -= 2;
    h = 2;
  }

  for (; h < K; h *= 2) {
    p0 = 0;
    p1 = L - 2 * h;

    while (p0 <= p1) {
      GRAIL_MERGE_LEFT(arr + p0, h, h, -h);
      p0 += 2 * h;
    }

    rest = L - p0;

    if (rest > h) {
      GRAIL_MERGE_LEFT(arr + p0, h, rest - h, -h);
    } else {
      GRAIL_ROTATE(arr + p0 - h, h, rest);
    }

    arr -= h;
  }

  restk = L % (2 * K);
  p = L - restk;

  if (restk <= K) {
    GRAIL_ROTATE(arr + p, restk, K);
  } else {
    GRAIL_MERGE_RIGHT(arr + p, K, restk - K, K);
  }

  while (p > 0) {
    p -= 2 * K;
    GRAIL_MERGE_RIGHT(arr + p, K, K, K);
  }
}

/*
  arr - starting array. arr[-lblock..-1] - buffer (if havebuf).
  lblock - length of regular blocks. First nblocks are stable sorted by 1st elements and key-coded
  keys - arrays of keys, in same order as blocks. key<midkey means stream A
  nblock2 are regular blocks from stream A. llast is length of last (irregular) block from stream B, that should go before nblock2 blocks.
  llast=0 requires nblock2=0 (no irregular blocks). llast>0, nblock2=0 is possible.
*/
static void GRAIL_MERGE_BUFFERS_LEFT(SORT_TYPE *keys, SORT_TYPE *midkey, SORT_TYPE *arr, int nblock,
                                     int lblock, int havebuf, int nblock2, int llast) {
  int l, prest, lrest, frest, pidx, cidx, fnext;

  if (nblock == 0) {
    l = nblock2 * lblock;

    if (havebuf) {
      GRAIL_MERGE_LEFT(arr, l, llast, -lblock);
    } else {
      GRAIL_MERGE_WITHOUT_BUFFER(arr, l, llast);
    }

    return;
  }

  lrest = lblock;
  frest = SORT_CMP_A(keys, midkey) < 0 ? 0 : 1;
  pidx = lblock;

  for (cidx = 1; cidx < nblock; cidx++, pidx += lblock) {
    prest = pidx - lrest;
    fnext = SORT_CMP_A(keys + cidx, midkey) < 0 ? 0 : 1;

    if (fnext == frest) {
      if (havebuf) {
        GRAIL_SWAP_N(arr + prest - lblock, arr + prest, lrest);
      }

      prest = pidx;
      lrest = lblock;
    } else {
      if (havebuf) {
        GRAIL_SMART_MERGE_WITH_BUFFER(arr + prest, &lrest, &frest, lblock, lblock);
      } else {
        GRAIL_SMART_MERGE_WITHOUT_BUFFER(arr + prest, &lrest, &frest, lblock);
      }
    }
  }

  prest = pidx - lrest;

  if (llast) {
    if (frest) {
      if (havebuf) {
        GRAIL_SWAP_N(arr + prest - lblock, arr + prest, lrest);
      }

      prest = pidx;
      lrest = lblock * nblock2;
      frest = 0;
    } else {
      lrest += lblock * nblock2;
    }

    if (havebuf) {
      GRAIL_MERGE_LEFT(arr + prest, lrest, llast, -lblock);
    } else {
      GRAIL_MERGE_WITHOUT_BUFFER(arr + prest, lrest, llast);
    }
  } else {
    if (havebuf) {
      GRAIL_SWAP_N(arr + prest, arr + (prest - lblock), lrest);
    }
  }
}

static void GRAIL_LAZY_STABLE_SORT(SORT_TYPE *arr, int L) {
  int m, h, p0, p1, rest;

  for (m = 1; m < L; m += 2) {
    if (SORT_CMP_A(arr + m - 1, arr + m) > 0) {
      GRAIL_SWAP1(arr + (m - 1), arr + m);
    }
  }

  for (h = 2; h < L; h *= 2) {
    p0 = 0;
    p1 = L - 2 * h;

    while (p0 <= p1) {
      GRAIL_MERGE_WITHOUT_BUFFER(arr + p0, h, h);
      p0 += 2 * h;
    }

    rest = L - p0;

    if (rest > h) {
      GRAIL_MERGE_WITHOUT_BUFFER(arr + p0, h, rest - h);
    }
  }
}

/*
  keys are on the left of arr. Blocks of length LL combined. We'll combine them in pairs
  LL and nkeys are powers of 2. (2*LL/lblock) keys are guarantied
*/
static void GRAIL_COMBINE_BLOCKS(SORT_TYPE *keys, SORT_TYPE *arr, int len, int LL, int lblock,
                                 int havebuf, SORT_TYPE *xbuf) {
  int M, b, NBlk, midkey, lrest, u, p, v, kc, nbl2, llast;
  SORT_TYPE *arr1;
  M = len / (2 * LL);
  lrest = len % (2 * LL);

  if (lrest <= LL) {
    len -= lrest;
    lrest = 0;
  }

  if (xbuf) {
    memcpy(xbuf, arr - lblock, lblock * sizeof(SORT_TYPE));
  }

  for (b = 0; b <= M; b++) {
    if (b == M && lrest == 0) {
      break;
    }

    arr1 = arr + b * 2 * LL;
    NBlk = (b == M ? lrest : 2 * LL) / lblock;
    BINARY_INSERTION_SORT(keys, NBlk + (b == M ? 1 : 0));
    midkey = LL / lblock;

    for (u = 1; u < NBlk; u++) {
      p = u - 1;

      for (v = u; v < NBlk; v++) {
        kc = SORT_CMP_A(arr1 + p * lblock, arr1 + v * lblock);

        if (kc > 0 || (kc == 0 && SORT_CMP_A(keys + p, keys + v) > 0)) {
          p = v;
        }
      }

      if (p != u - 1) {
        GRAIL_SWAP_N(arr1 + (u - 1)*lblock, arr1 + p * lblock, lblock);
        GRAIL_SWAP1(keys + (u - 1), keys + p);

        if (midkey == u - 1 || midkey == p) {
          midkey ^= (u - 1)^p;
        }
      }
    }

    nbl2 = llast = 0;

    if (b == M) {
      llast = lrest % lblock;
    }

    if (llast != 0) {
      while (nbl2 < NBlk && SORT_CMP_A(arr1 + NBlk * lblock, arr1 + (NBlk - nbl2 - 1)*lblock) < 0) {
        nbl2++;
      }
    }

    if (xbuf) {
      GRAIL_MERGE_BUFFERS_LEFT_WITH_X_BUF(keys, keys + midkey, arr1, NBlk - nbl2, lblock, nbl2, llast);
    } else {
      GRAIL_MERGE_BUFFERS_LEFT(keys, keys + midkey, arr1, NBlk - nbl2, lblock, havebuf, nbl2, llast);
    }
  }

  if (xbuf) {
    for (p = len; --p >= 0;) {
      arr[p] = arr[p - lblock];
    }

    memcpy(arr - lblock, xbuf, lblock * sizeof(SORT_TYPE));
  } else if (havebuf) while (--len >= 0) {
      GRAIL_SWAP1(arr + len, arr + len - lblock);
    }
}


static void GRAIL_COMMON_SORT(SORT_TYPE *arr, int Len, SORT_TYPE *extbuf, int LExtBuf) {
  int lblock, nkeys, findkeys, ptr, cbuf, lb, nk;
  int havebuf, chavebuf;
  long long s;

  if (Len < 16) {
    BINARY_INSERTION_SORT(arr, Len);
    return;
  }

  lblock = 1;

  while (lblock * lblock < Len) {
    lblock *= 2;
  }

  nkeys = (Len - 1) / lblock + 1;
  findkeys = GRAIL_FIND_KEYS(arr, Len, nkeys + lblock);
  havebuf = 1;

  if (findkeys < nkeys + lblock) {
    if (findkeys < 4) {
      GRAIL_LAZY_STABLE_SORT(arr, Len);
      return;
    }

    nkeys = lblock;

    while (nkeys > findkeys) {
      nkeys /= 2;
    }

    havebuf = 0;
    lblock = 0;
  }

  ptr = lblock + nkeys;
  cbuf = havebuf ? lblock : nkeys;

  if (havebuf) {
    GRAIL_BUILD_BLOCKS(arr + ptr, Len - ptr, cbuf, extbuf, LExtBuf);
  } else {
    GRAIL_BUILD_BLOCKS(arr + ptr, Len - ptr, cbuf, NULL, 0);
  }

  /* 2*cbuf are built */
  while (Len - ptr > (cbuf *= 2)) {
    lb = lblock;
    chavebuf = havebuf;

    if (!havebuf) {
      if (nkeys > 4 && nkeys / 8 * nkeys >= cbuf) {
        lb = nkeys / 2;
        chavebuf = 1;
      } else {
        nk = 1;
        s = (long long)cbuf * findkeys / 2;

        while (nk < nkeys && s != 0) {
          nk *= 2;
          s /= 8;
        }

        lb = (2 * cbuf) / nk;
      }
    }

    GRAIL_COMBINE_BLOCKS(arr, arr + ptr, Len - ptr, cbuf, lb, chavebuf, chavebuf
                         && lb <= LExtBuf ? extbuf : NULL);
  }

  BINARY_INSERTION_SORT(arr, ptr);
  GRAIL_MERGE_WITHOUT_BUFFER(arr, ptr, Len - ptr);
}

static void GRAIL_SORT(SORT_TYPE *arr, size_t Len) {
  GRAIL_COMMON_SORT(arr, Len, NULL, 0);
}

static void GRAIL_SORT_FIXED_BUFFER(SORT_TYPE *arr, size_t Len) {
  SORT_TYPE ExtBuf[GRAIL_EXT_BUFFER_LENGTH];
  GRAIL_COMMON_SORT(arr, Len, ExtBuf, GRAIL_EXT_BUFFER_LENGTH);
}

static void GRAIL_SORT_DYN_BUFFER(SORT_TYPE *arr, size_t Len) {
  int L = 1;
  SORT_TYPE *ExtBuf;

  while (L * L < Len) {
    L *= 2;
  }

  ExtBuf = (SORT_TYPE*)malloc(L * sizeof(SORT_TYPE));

  if (ExtBuf == NULL) {
    GRAIL_SORT_FIXED_BUFFER(arr, Len);
  } else {
    GRAIL_COMMON_SORT(arr, Len, ExtBuf, L);
    free(ExtBuf);
  }
}

/****** classic MergeInPlace *************/

static void GRAIL_REC_MERGE(SORT_TYPE *A, int L1, int L2) {
  int K, k1, k2, m1, m2;

  if (L1 < 3 || L2 < 3) {
    GRAIL_MERGE_WITHOUT_BUFFER(A, L1, L2);
    return;
  }

  if (L1 < L2) {
    K = L1 + L2 / 2;
  } else {
    K = L1 / 2;
  }

  k1 = k2 = GRAIL_BIN_SEARCH_LEFT(A, L1, A + K);

  if (k2 < L1 && SORT_CMP_A(A + k2, A + K) == 0) {
    k2 = GRAIL_BIN_SEARCH_RIGHT(A + k1, L1 - k1, A + K) + k1;
  }

  m1 = GRAIL_BIN_SEARCH_LEFT(A + L1, L2, A + K);
  m2 = m1;

  if (m2 < L2 && SORT_CMP_A(A + L1 + m2, A + K) == 0) {
    m2 = GRAIL_BIN_SEARCH_RIGHT(A + L1 + m1, L2 - m1, A + K) + m1;
  }

  if (k1 == k2) {
    GRAIL_ROTATE(A + k2, L1 - k2, m2);
  } else {
    GRAIL_ROTATE(A + k1, L1 - k1, m1);

    if (m2 != m1) {
      GRAIL_ROTATE(A + (k2 + m1), L1 - k2, m2 - m1);
    }
  }

  GRAIL_REC_MERGE(A + (k2 + m2), L1 - k2, L2 - m2);
  GRAIL_REC_MERGE(A, k1, m1);
}

static void REC_STABLE_SORT(SORT_TYPE *arr, size_t L) {
  int m, h, p0, p1, rest;

  for (m = 1; m < L; m += 2) {
    if (SORT_CMP_A(arr + m - 1, arr + m) > 0) {
      GRAIL_SWAP1(arr + (m - 1), arr + m);
    }
  }

  for (h = 2; h < L; h *= 2) {
    p0 = 0;
    p1 = L - 2 * h;

    while (p0 <= p1) {
      GRAIL_REC_MERGE(arr + p0, h, h);
      p0 += 2 * h;
    }

    rest = L - p0;

    if (rest > h) {
      GRAIL_REC_MERGE(arr + p0, h, rest - h);
    }
  }
}

/* Bubble sort implementation based on Wikipedia article
   https://en.wikipedia.org/wiki/Bubble_sort
*/
void BUBBLE_SORT(SORT_TYPE *dst, const size_t size) {
  size_t n = size;

  while (n) {
    size_t i, newn = 0U;

    for (i = 1U; i < n; ++i) {
      if (SORT_CMP(dst[i - 1U], dst[i]) > 0) {
        SORT_SWAP(dst[i - 1U], dst[i]);
        newn = i;
      }
    }

    n = newn;
  }
}

#undef QUICK_SORT
#undef MEDIAN
#undef SORT_CONCAT
#undef SORT_MAKE_STR1
#undef SORT_MAKE_STR
#undef SORT_NAME
#undef SORT_TYPE
#undef SORT_CMP
#undef TEMP_STORAGE_T
#undef TIM_SORT_RUN_T
#undef PUSH_NEXT
#undef SORT_SWAP
#undef SORT_CONCAT
#undef SORT_MAKE_STR1
#undef SORT_MAKE_STR
#undef BINARY_INSERTION_FIND
#undef BINARY_INSERTION_SORT_START
#undef BINARY_INSERTION_SORT
#undef REVERSE_ELEMENTS
#undef COUNT_RUN
#undef TIM_SORT
#undef TIM_SORT_RESIZE
#undef TIM_SORT_COLLAPSE
#undef TIM_SORT_RUN_T
#undef TEMP_STORAGE_T
#undef MERGE_SORT
#undef MERGE_SORT_IN_PLACE
#undef MERGE_SORT_IN_PLACE_RMERGE
#undef MERGE_SORT_IN_PLACE_BACKMERGE
#undef MERGE_SORT_IN_PLACE_FRONTMERGE
#undef MERGE_SORT_IN_PLACE_ASWAP
#undef GRAIL_SWAP1
#undef REC_STABLE_SORT
#undef GRAIL_REC_MERGE
#undef GRAIL_SORT_DYN_BUFFER
#undef GRAIL_SORT_FIXED_BUFFER
#undef GRAIL_COMMON_SORT
#undef GRAIL_SORT
#undef GRAIL_COMBINE_BLOCKS
#undef GRAIL_LAZY_STABLE_SORT
#undef GRAIL_MERGE_WITHOUT_BUFFER
#undef GRAIL_ROTATE
#undef GRAIL_BIN_SEARCH_LEFT
#undef GRAIL_BUILD_BLOCKS
#undef GRAIL_FIND_KEYS
#undef GRAIL_MERGE_BUFFERS_LEFT_WITH_X_BUF
#undef GRAIL_BIN_SEARCH_RIGHT
#undef GRAIL_MERGE_BUFFERS_LEFT
#undef GRAIL_SMART_MERGE_WITH_X_BUF
#undef GRAIL_MERGE_LEFT_WITH_X_BUF
#undef GRAIL_SMART_MERGE_WITHOUT_BUFFER
#undef GRAIL_SMART_MERGE_WITH_BUFFER
#undef GRAIL_MERGE_RIGHT
#undef GRAIL_MERGE_LEFT
#undef GRAIL_SWAP_N
#undef SQRT_SORT
#undef SQRT_SORT_BUILD_BLOCKS
#undef SQRT_SORT_MERGE_BUFFERS_LEFT_WITH_X_BUF
#undef SQRT_SORT_MERGE_DOWN
#undef SQRT_SORT_MERGE_LEFT_WITH_X_BUF
#undef SQRT_SORT_MERGE_RIGHT
#undef SQRT_SORT_SWAP_N
#undef SQRT_SORT_SWAP_1
#undef SQRT_SORT_SMART_MERGE_WITH_X_BUF
#undef SQRT_SORT_SORT_INS
#undef SQRT_SORT_COMBINE_BLOCKS
#undef SQRT_SORT_COMMON_SORT
#undef SORT_CMP_A
#undef BUBBLE_SORT

