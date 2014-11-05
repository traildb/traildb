import os

from collections import namedtuple
from ctypes import c_char, c_char_p, c_ubyte, c_int, c_void_p
from ctypes import c_uint, c_uint8, c_uint32, c_uint64
from ctypes import CDLL, CFUNCTYPE, POINTER
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
api(lib.tdb_cons_finalize, [tdb_cons, c_uint64], c_int)

api(lib.tdb_open, [c_char_p], tdb)
api(lib.tdb_close, [tdb])

api(lib.tdb_lexicon_size, [tdb, tdb_field], c_int)

api(lib.tdb_get_field, [tdb, c_char_p], c_uint)
api(lib.tdb_get_field_name, [tdb, tdb_field], c_char_p)

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

api(lib.tdb_decode_trail, [tdb, c_uint64, POINTER(c_uint32), c_uint32, c_int], c_uint32)
api(lib.tdb_fold, [tdb, tdb_fold_fn, c_void_p], c_void_p)

def hexcookie(cookie):
    if isinstance(cookie, basestring):
        return cookie
    return ''.join('%.2x' % x for x in cookie[:16])

def rawcookie(cookie):
    if isinstance(cookie, basestring):
        return (c_ubyte * 16).from_buffer_copy(cookie.decode('hex'))
    return cookie

class TrailDBError(Exception):
    pass

class TrailDBConstructor(object):
    def __init__(self, path, fields=()):
        self._cons = cons = lib.tdb_cons_new(path, '\x00'.join(fields), len(fields))
        self.path = path
        self.fields = fields

    def __del__(self):
        lib.tdb_cons_free(self._cons)

    def add(self, cookie, time, values=()):
        if isinstance(time, datetime):
            time = int(time.strftime('%s'))
        f = lib.tdb_cons_add(self._cons, rawcookie(cookie), time, '\x00'.join(values))
        if f:
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

    def trail(self, id, expand=True, ptime=False):
        db, cls = self._db, self._evcls
        buf, size = self._trail_buf, self._trail_buf_size

        while True:
            num = lib.tdb_decode_trail(db, id, buf, size, 0)
            if num == 0:
                raise IndexError("Cookie index out of range")
            elif num == size:
                buf, size = self._grow_buffer()
            else:
                break

        if expand:
            value = lambda f, v: lib.tdb_get_value(db, f, v)
        else:
            value = lambda f, v: v

        def gen(i=0):
            while i < num:
                tstamp = datetime.fromtimestamp(buf[i]) if ptime else buf[i]
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
            raise TrailDBError("No such value")
        return item >> 8

    def value(self, fieldish, val):
        field = self.field(fieldish)
        value = lib.tdb_get_value(self._db, field, val)
        if value is None:
            raise TrailDBError(lib.tdb_error(self._db))
        return value

    def cookie(self, id):
        cookie = lib.tdb_get_cookie(self._db, id)
        if cookie:
            return hexcookie(cookie)
        raise IndexError("Cookie index out of range")

    def cookie_id(self, cookie):
        cookie_id = lib.tdb_get_cookie_id(self._db, rawcookie(cookie))
        if cookie_id < self.num_cookies:
            return cookie_id
        raise IndexError("Cookie '%s' not found" % cookie)

    def has_cookie_index(self):
        return True if lib.tdb_has_cookie_index(self._db) else False

    def time_range(self, ptime=False):
        tmin = lib.tdb_min_timestamp(self._db)
        tmax = lib.tdb_max_timestamp(self._db)
        if ptime:
            return datetime.fromtimestamp(tmin), datetime.fromtimestamp(tmax)
        return tmin, tmax
