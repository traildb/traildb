import os

from collections import namedtuple
from ctypes import c_char, c_char_p, c_ubyte, c_void_p, c_int
from ctypes import c_uint, c_uint8, c_uint32, c_uint64
from ctypes import CDLL, Structure, POINTER
from datetime import datetime

cd = os.path.dirname(os.path.abspath(__file__))
lib = CDLL(os.path.join(cd, 'libtraildb.so'))

def api(fun, args, res=None):
    fun.argtypes = args
    fun.restype = res

tdb_field = c_uint8
tdb_val   = c_uint32
tdb_item  = c_uint32

api(lib.tdb_cons_new, [c_char_p, POINTER(c_char), c_uint32], c_void_p)
api(lib.tdb_cons_free, [c_void_p])
api(lib.tdb_cons_add, [c_void_p, POINTER(c_ubyte), c_uint32, POINTER(c_char)], c_int)
api(lib.tdb_cons_finalize, [c_void_p, c_uint64], c_int)

api(lib.tdb_open, [c_char_p], c_void_p)
api(lib.tdb_close, [c_void_p])

api(lib.tdb_lexicon_size, [c_void_p, tdb_field, POINTER(c_uint32)], c_int)

api(lib.tdb_get_field, [c_void_p, c_char_p], c_uint)
api(lib.tdb_get_field_name, [c_void_p, tdb_field], c_char_p)

api(lib.tdb_get_item, [c_void_p, tdb_field, c_char_p], c_uint)
api(lib.tdb_get_value, [c_void_p, tdb_field, tdb_val], c_char_p)
api(lib.tdb_get_item_value, [c_void_p, tdb_item], c_char_p)

api(lib.tdb_get_cookie, [c_void_p, c_uint64], POINTER(c_ubyte))
api(lib.tdb_get_cookie_id, [c_void_p, POINTER(c_ubyte)], c_int)
api(lib.tdb_has_cookie_index, [c_void_p], c_int)

api(lib.tdb_error, [c_void_p], c_char_p)

api(lib.tdb_num_cookies, [c_void_p], c_uint64)
api(lib.tdb_num_events, [c_void_p], c_uint64)
api(lib.tdb_num_fields, [c_void_p], c_uint32)
api(lib.tdb_min_timestamp, [c_void_p], c_uint32)
api(lib.tdb_max_timestamp, [c_void_p], c_uint32)

api(lib.tdb_decode_trail, [c_void_p, c_uint64, POINTER(c_uint32), c_uint32, c_int], c_uint32)

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
        self._evcls = namedtuple('event', ['time'] + self.fields, rename=True)
        self._trail_buf_size = 0
        self._grow_buffer()

    def __del__(self):
        lib.tdb_close(self._db)

    def _grow_buffer(self, increment=1000000):
        self._trail_buf_size += increment
        self._trail_buf = (c_uint32 * self._trail_buf_size)()
        return self._trail_buf, self._trail_buf_size

    def __contains__(self, cookie):
        pass # XXX: like on get_cookie_id but don't use an exception

    def __len__(self):
        return self.num_cookies

    def __getitem__(self, id):
        return self.trail(id)

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
            value = lambda f, v: lib.tdb_get_value(db, f - 1, v)
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

    def time_range(self, ptime=False):
        tmin = lib.tdb_min_timestamp(self._db)
        tmax = lib.tdb_max_timestamp(self._db)
        if ptime:
            return datetime.fromtimestamp(tmin), datetime.fromtimestamp(tmax)
        return tmin, tmax

    def has_cookie_index(self):
        return True if lib.tdb_has_cookie_index(self._db) else False

    def cookie(self, id):
        c = lib.tdb_get_cookie(self._db, id)
        if c:
            return hexcookie(c)
        raise IndexError("Cookie index out of range")

    def cookie_id(self, cookie):
        i = lib.tdb_get_cookie_id(self._db, rawcookie(cookie))
        if i >= 0:
            return i
        elif i == -2: # NB: no cookie index
            # XXX: how do we tell if was too small vs not created?
            for i in xrange(len(self)):
                if self.cookie(i) == cookie:
                    return i
        raise IndexError("Cookie '%s' not found" % cookie)

    def value(self, field, val):
        # XXX: timestamps? 1-based indexing is really strange here
        v = lib.tdb_get_value(self._db, field - 1, val)
        if v:
            return v
        raise TrailDBError("Field %d, val %d does not exist" % (field, val))

    def lexicon(self, field):
        # XXX: 1-based indexing is really strange here
        if isinstance(field, basestring):
            return self.lexicon(self.fields.index(field) + 1)
        return [self.value(field, i) for i in xrange(1, self.lexicon_size(field) + 1)]

    def lexicon_size(self, field):
        # XXX: 1-based indexing is really strange here
        size = (c_uint)()
        if lib.tdb_lexicon_size(self._db, field - 1, size):
            raise TrailDBError(lib.tdb_error(self._db))
        return size.value
