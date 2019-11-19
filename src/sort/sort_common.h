/* Copyright (c) 2010-2014 Christopher Swenson. */
/* Copyright (c) 2012 Vojtech Fried. */
/* Copyright (c) 2012 Google Inc. All Rights Reserved. */

#ifndef SORT_COMMON_H
#define SORT_COMMON_H

#ifndef MAX
#define MAX(x,y) (((x) > (y) ? (x) : (y)))
#endif

#ifndef MIN
#define MIN(x,y) (((x) < (y) ? (x) : (y)))
#endif

static int compute_minrun(const uint64_t);

/* From http://oeis.org/classic/A102549 */
static const uint64_t shell_gaps[48] = {1, 4, 10, 23, 57, 132, 301, 701, 1750, 4376, 10941, 27353, 68383, 170958, 427396, 1068491, 2671228, 6678071, 16695178, 41737946, 104344866, 260862166, 652155416, 1630388541, 4075971353LL, 10189928383LL, 25474820958LL, 63687052396LL, 159217630991LL, 398044077478LL, 995110193696LL, 2487775484241LL, 6219438710603LL, 15548596776508LL, 38871491941271LL, 97178729853178LL, 242946824632946LL, 607367061582366LL, 1518417653955916LL, 3796044134889791LL, 9490110337224478LL, 23725275843061196LL, 59313189607652991LL, 148282974019132478LL, 370707435047831196LL, 926768587619577991LL, 2316921469048944978LL, 5792303672622362446LL};

#ifndef CLZ
#ifdef __GNUC__
#define CLZ __builtin_clzll
#else

static int clzll(uint64_t);

/* adapted from Hacker's Delight */
static int clzll(uint64_t x) {
  int n;

  if (x == 0) {
    return 64;
  }

  n = 0;

  if (x <= 0x00000000FFFFFFFFL) {
    n = n + 32;
    x = x << 32;
  }

  if (x <= 0x0000FFFFFFFFFFFFL) {
    n = n + 16;
    x = x << 16;
  }

  if (x <= 0x00FFFFFFFFFFFFFFL) {
    n = n + 8;
    x = x << 8;
  }

  if (x <= 0x0FFFFFFFFFFFFFFFL) {
    n = n + 4;
    x = x << 4;
  }

  if (x <= 0x3FFFFFFFFFFFFFFFL) {
    n = n + 2;
    x = x << 2;
  }

  if (x <= 0x7FFFFFFFFFFFFFFFL) {
    n = n + 1;
  }

  return n;
}

#define CLZ clzll
#endif
#endif

static __inline int compute_minrun(const uint64_t size) {
  const int top_bit = 64 - CLZ(size);
  const int shift = MAX(top_bit, 6) - 6;
  const int minrun = size >> shift;
  const uint64_t mask = (1ULL << shift) - 1;

  if (mask & size) {
    return minrun + 1;
  }

  return minrun;
}

static __inline size_t rbnd(size_t len) {
  int k;

  if (len < 16) {
    return 2;
  }

  k = 62 - CLZ(len);
  return 1ULL << ((2 * k) / 3);
}

#endif /* SORT_COMMON_H */
