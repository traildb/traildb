#ifndef __TDB_TEST_H__
#define __TDB_TEST_H__

#include <stdlib.h>
#include <assert.h>

#include <traildb.h>

static inline void test_cons_settings(tdb_cons *cons)
{
    if (getenv("TDB_CONS_NO_BIGRAMS")) {
        assert(tdb_cons_set_opt(cons,
                                TDB_OPT_CONS_NO_BIGRAMS,
                                opt_val(1)) == 0);
    } else {
        assert(tdb_cons_set_opt(cons,
                                TDB_OPT_CONS_NO_BIGRAMS,
                                opt_val(0)) == 0);
    }
#ifdef __HAVE_ARCHIVE_H__
    if (getenv("TDB_CONS_OUTPUT_FORMAT")){
        assert(tdb_cons_set_opt(cons,
                                TDB_OPT_CONS_OUTPUT_FORMAT,
                                opt_val(TDB_OPT_CONS_OUTPUT_FORMAT_PACKAGE)) == 0);
        return;
    }
#endif
    assert(tdb_cons_set_opt(cons,
                            TDB_OPT_CONS_OUTPUT_FORMAT,
                            opt_val(TDB_OPT_CONS_OUTPUT_FORMAT_DIR)) == 0);
}

#endif /* __TDB_TEST_H__ */

