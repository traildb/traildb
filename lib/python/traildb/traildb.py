import os

from collections import namedtuple, defaultdict
from collections import Mapping
from ctypes import c_char, c_char_p, c_ubyte, c_int, c_void_p
from ctypes import c_uint, c_uint8, c_uint32, c_uint64
from ctypes import CDLL, CFUNCTYPE, POINTER, string_at, byref
from datetime import datetime

cd = os.path.dirname(os.path.abspath(__file__))
lib = CDLL(os.path.join(cd, 'libtraildb.so'))

def api(fun, args, res=None):
    fun.argtypes = args
    fun.restype = res

tdb         = c_void_p
tdb_cons    = c_void_p
tdb_field   = c_uint8
tdb_val     = c_uint32
tdb_item    = c_uint32
tdb_fold_fn = CFUNCTYPE(c_void_p, tdb, c_uint64, POINTER(tdb_item), c_void_p)

api(lib.tdb_cons_new, [c_char_p, POINTER(c_char), c_uint32], tdb_cons)
api(lib.tdb_cons_free, [tdb_cons])
api(lib.tdb_cons_add, [tdb_cons, POINTER(c_ubyte), c_uint32, POINTER(c_char)], c_int)
api(lib.tdb_cons_append, [tdb_cons, tdb], c_int)
api(lib.tdb_cons_finalize, [tdb_cons, c_uint64], c_int)

api(lib.tdb_open, [c_char_p], tdb)
api(lib.tdb_close, [tdb])

api(lib.tdb_lexicon_size, [tdb, tdb_field], c_int)

api(lib.tdb_get_field, [tdb, c_char_p], c_uint)
api(lib.tdb_get_field_name, [tdb, tdb_field], c_char_p)
api(lib.tdb_field_has_overflow_vals, [tdb, tdb_field], c_int)

api(lib.tdb_get_item, [tdb, tdb_field, c_char_p], c_uint)
api(lib.tdb_get_value, [tdb, tdb_field, tdb_val], c_char_p)
api(lib.tdb_get_item_value, [tdb, tdb_item], c_char_p)

api(lib.tdb_get_cookie, [tdb, c_uint64], POINTER(c_ubyte))
api(lib.tdb_get_cookie_id, [tdb, POINTER(c_ubyte)], c_uint64)
api(lib.tdb_has_cookie_index, [tdb], c_int)

api(lib.tdb_error, [tdb], c_char_p)

api(lib.tdb_num_cookies, [tdb], c_uint64)
api(lib.tdb_num_events, [tdb], c_uint64)
api(lib.tdb_num_fields, [tdb], c_uint32)
api(lib.tdb_min_timestamp, [tdb], c_uint32)
api(lib.tdb_max_timestamp, [tdb], c_uint32)

api(lib.tdb_split, [tdb, c_uint, c_char_p, c_uint64], c_int)

api(lib.tdb_set_filter, [tdb, POINTER(c_uint32), c_uint32], c_int)
api(lib.tdb_get_filter, [tdb, POINTER(c_uint32)], POINTER(c_uint32))

api(lib.tdb_decode_trail, [tdb, c_uint64, POINTER(c_uint32), c_uint32, c_int], c_uint32)
api(lib.tdb_decode_trail_filtered,
    [tdb, c_uint64, POINTER(c_uint32), c_uint32, POINTER(c_uint32), c_uint32, c_int],
    c_uint32)

api(lib.tdb_fold, [tdb, tdb_fold_fn, c_void_p], c_void_p)

def hexcookie(cookie):
    if isinstance(cookie, basestring):
        return cookie
    return string_at(cookie, 16).encode('hex')

def rawcookie(cookie):
    if isinstance(cookie, basestring):
        return (c_ubyte * 16).from_buffer_copy(cookie.decode('hex'))
    return cookie

def nullterm(strs, size):
    return '\x00'.join(strs) + (size - len(strs) + 1) * '\x00'

class TrailDBError(Exception):
    pass

class TrailDBConstructor(object):
    def __init__(self, path, ofields=()):
        if not path:
            raise TrailDBError("Path is required")
        n = len(ofields)
        self._cons = cons = lib.tdb_cons_new(path, nullterm(ofields, n), n)
        self.path = path
        self.ofields = ofields

    def __del__(self):
        if hasattr(self, '_cons'):
            lib.tdb_cons_free(self._cons)

    def add(self, cookie, time, values=()):
        if isinstance(time, datetime):
            time = int(time.strftime('%s'))
        n = len(self.ofields)
        f = lib.tdb_cons_add(self._cons, rawcookie(cookie), time, nullterm(values, n))
        if f:
            raise TrailDBError("Too many values: %s" % values[f])

    def append(self, db):
        f = lib.tdb_cons_append(self._cons, db._db)
        if f < 0:
            raise TrailDBError("Wrong number of fields: %d" % db.num_fields)
        if f > 0:
            raise TrailDBError("Too many values: %s" % values[f])

    def finalize(self, flags=0):
        r = lib.tdb_cons_finalize(self._cons, flags)
        if r:
            raise TrailDBError("Could not finalize (%d)" % r)
        return TrailDB(self.path)

class TrailDB(object):
    def __init__(self, path):
        self._db = db = lib.tdb_open(path)
        if not db:
            raise TrailDBError("Could not open %s" % path)
        self.num_cookies = lib.tdb_num_cookies(db)
        self.num_events = lib.tdb_num_events(db)
        self.num_fields = lib.tdb_num_fields(db)
        self.fields = [lib.tdb_get_field_name(db, i) for i in xrange(self.num_fields)]
        self._evcls = namedtuple('event', self.fields, rename=True)
        self._trail_buf_size = 0
        self._grow_buffer()

    def __del__(self):
        lib.tdb_close(self._db)

    def _grow_buffer(self, increment=1000000):
        self._trail_buf_size += increment
        self._trail_buf = (c_uint32 * self._trail_buf_size)()
        return self._trail_buf, self._trail_buf_size

    def __contains__(self, cookieish):
        try:
            self[cookieish]
            return True
        except IndexError:
            return False

    def __getitem__(self, cookieish):
        if isinstance(cookieish, basestring):
            return self.trail(self.cookie_id(cookieish))
        return self.trail(cookieish)

    def __len__(self):
        return self.num_cookies

    def crumbs(self, **kwds):
        for i in xrange(len(self)):
            yield self.cookie(i), self.trail(i, **kwds)

    def trail(self, id, expand=True, ptime=False, filter_expr=None):
        db, cls = self._db, self._evcls
        buf, size = self._trail_buf, self._trail_buf_size
        q = None

        if id >= self.num_cookies:
            raise IndexError("Cookie index out of range")

        if filter_expr != None:
            q = self._parse_filter(filter_expr)

        while True:
            if q:
                num = lib.tdb_decode_trail_filtered(db,
                                                    id,
                                                    buf,
                                                    size,
                                                    0,
                                                    q,
                                                    len(q))
            else:
                num = lib.tdb_decode_trail(db, id, buf, size, 0)

            if num == size:
                buf, size = self._grow_buffer()
            else:
                break

        if expand:
            value = lambda f, v: lib.tdb_get_value(db, f, v)
        else:
            value = lambda f, v: v

        def gen(i=0):
            while i < num:
                tstamp = datetime.utcfromtimestamp(buf[i]) if ptime else buf[i]
                values = []
                i += 1
                while i < num and buf[i]:
                    values.append(value(buf[i] & 255, buf[i] >> 8))
                    i += 1
                i += 1
                yield cls(tstamp, *values)
        return list(gen())

    def fold(self, fun, acc=None):
        db, cls, N = self._db, self._evcls, self.num_fields
        def fn(tdb, id, items, _):
            fun(self, id, cls(items[0], *(i >> 8 for i in items[1:N])), acc)
        lib.tdb_fold(db, tdb_fold_fn(fn), None)
        return acc

    def field(self, fieldish):
        if isinstance(fieldish, basestring):
            return self.fields.index(fieldish)
        return fieldish

    def lexicon(self, fieldish):
        field = self.field(fieldish)
        return [self.value(field, i) for i in xrange(self.lexicon_size(field))]

    def lexicon_size(self, fieldish):
        field = self.field(fieldish)
        value = lib.tdb_lexicon_size(self._db, field)
        if not value:
            raise TrailDBError(lib.tdb_error(self._db))
        return value

    def val(self, fieldish, value):
        field = self.field(fieldish)
        item = lib.tdb_get_item(self._db, field, value)
        if not item:
            raise TrailDBError("No such value: '%s'" % value)
        return item >> 8

    def value(self, fieldish, val):
        field = self.field(fieldish)
        value = lib.tdb_get_value(self._db, field, val)
        if value is None:
            raise TrailDBError(lib.tdb_error(self._db))
        return value

    def cookie(self, id, raw=False):
        cookie = lib.tdb_get_cookie(self._db, id)
        if cookie:
            if raw:
                return string_at(cookie, 16)
            else:
                return hexcookie(cookie)
        raise IndexError("Cookie index out of range")

    def cookie_id(self, cookie):
        cookie_id = lib.tdb_get_cookie_id(self._db, rawcookie(cookie))
        if cookie_id < self.num_cookies:
            return cookie_id
        raise IndexError("Cookie '%s' not found" % cookie)

    def has_cookie_index(self):
        return True if lib.tdb_has_cookie_index(self._db) else False

    def has_overflow_vals(self, fieldish):
        field = self.field(fieldish)
        return lib.tdb_field_has_overflow_vals(self._db, field) != 0

    def time_range(self, ptime=False):
        tmin = lib.tdb_min_timestamp(self._db)
        tmax = lib.tdb_max_timestamp(self._db)
        if ptime:
            return datetime.utcfromtimestamp(tmin), datetime.utcfromtimestamp(tmax)
        return tmin, tmax

    def split(self, num_parts, fmt='a.%02d.tdb', flags=0):
        ret = lib.tdb_split(self._db, num_parts, fmt, flags)
        if ret:
            raise TrailDBError("Could not split into %d parts" % num_parts)
        return [TrailDB(fmt % n) for n in xrange(num_parts)]

    def _parse_filter(self, filter_expr):
        # filter_expr syntax in CNF:
        # CNF is a list of clauses. A clause is a dictionary, a mapping from
        # fields to a list of allowed field values. A negative value can be
        # expressed with a dictionary {'is_negative': True, 'value': value}.
        #
        # Example:
        #
        # [
        #   {'network': ['Google', {'is_negative': True, 'value': 'Yahoo'}],
        #    'browser': ['Chrome']},
        #   {'is_valid': ['1']}
        # ]
        def item(value, field):
            return lib.tdb_get_item(self._db, field, value)

        def parse_clause(clause_expr):
            clause = [0]
            for field_str, values in clause_expr.iteritems():
                field = self.field(field_str)
                for value in values:
                    if isinstance(value, Mapping):
                        clause.append(1 if value['is_negative'] else 0)
                        clause.append(item(value['value'], field))
                    else:
                        clause.extend((0, item(value, field)))
            clause[0] = len(clause) - 1
            return clause

        q = []
        for clause in filter_expr:
            q.extend(parse_clause(clause))

        return (c_uint32 * len(q))(*q)

    def set_filter(self, filter_expr):
        q = self._parse_filter(filter_expr)
        print 'fu', list(q)
        if lib.tdb_set_filter(self._db, q, len(q)):
            raise TrailDBError("Setting filter failed (out of memory?)")

    def get_filter(self):

        def construct_clause(clause_arr):
            clause = defaultdict(list)
            for i in range(0, len(clause_arr), 2):
                is_negative = clause_arr[i]
                item = clause_arr[i + 1]
                field = item & 255
                field_str = self.fields[field]
                val = item >> 8
                value = lib.tdb_get_value(self._db, field, val)
                if is_negative:
                    clause[field_str].append({'is_negative': True,
                                              'value': value})
                else:
                    clause[field_str].append(value)
            return clause

        filter_len = c_uint32(0)
        filter_arr = lib.tdb_get_filter(self._db, byref(filter_len))
        q = []
        i = 0
        while i < filter_len.value:
            clause_len = filter_arr[i]
            q.append(construct_clause(filter_arr[i + 1:i + 1 + clause_len]))
            i += 1 + clause_len

        return q




