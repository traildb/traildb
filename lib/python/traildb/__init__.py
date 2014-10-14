import os

from collections import namedtuple
from ctypes import CDLL, c_char_p, c_ubyte, POINTER, c_void_p, c_int, c_uint
from datetime import datetime

cd = os.path.dirname(os.path.abspath(__file__))
lib = CDLL(os.path.join(cd, '_traildb.so'))

def api(fun, args, res=None):
    fun.argtypes = args
    fun.restype = res

api(lib.tdb_open, [c_char_p], c_void_p)
api(lib.tdb_close, [c_void_p])

api(lib.tdb_lexicon_size, [c_void_p, c_uint, POINTER(c_uint)], c_int)

api(lib.tdb_get_field, [c_void_p, c_char_p], c_uint)
api(lib.tdb_get_field_name, [c_void_p, c_uint], c_char_p)

api(lib.tdb_get_val, [c_void_p, c_uint, c_char_p], c_uint)
api(lib.tdb_get_value, [c_void_p, c_uint, c_uint], c_char_p)
api(lib.tdb_get_item_value, [c_void_p, c_uint], c_char_p)

api(lib.tdb_get_cookie, [c_void_p, c_uint], POINTER(c_ubyte))
api(lib.tdb_get_cookie_id, [c_void_p, c_char_p], c_int)
api(lib.tdb_has_cookie_index, [c_void_p], c_int)

api(lib.tdb_error, [c_void_p], c_char_p)

api(lib.tdb_num_cookies, [c_void_p], c_uint)
api(lib.tdb_num_events, [c_void_p], c_uint)
api(lib.tdb_num_fields, [c_void_p], c_uint)
api(lib.tdb_min_timestamp, [c_void_p], c_uint)
api(lib.tdb_max_timestamp, [c_void_p], c_uint)

api(lib.tdb_decode_trail, [c_void_p, c_uint, POINTER(c_uint), c_uint, c_int], c_uint)

class TrailDB(object):
    def __init__(self, path):
        self._db = db = lib.tdb_open(path)
        self.num_cookies = lib.tdb_num_cookies(db)
        self.num_events = lib.tdb_num_events(db)
        self.num_fields = lib.tdb_num_fields(db)
        self.fields = [lib.tdb_get_field_name(db, i) for i in xrange(self.num_fields)]
        self._evcls = namedtuple('event', ['time'] + self.fields)
        self._trail_buf_size = 0
        self._grow_buffer()

    def __del__(self):
        lib.tdb_close(self._db)

    def _grow_buffer(self, increment=1000000):
        self._trail_buf_size += increment
        self._trail_buf = (c_uint * self._trail_buf_size)()
        return self._trail_buf, self._trail_buf_size

    def __len__(self):
        return self.num_cookies

    def __getitem__(self, id):
        return self.trail(id)

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
                    values.append(value(buf[i] & 255 - 1, buf[i] >> 8))
                    i += 1
                i += 1
                yield cls(tstamp, *values)
        return gen()

    def cookie(self, id, raw=False):
        c = lib.tdb_get_cookie(self._db, id)
        if c:
            if raw:
                return c[:16]
            return ''.join('%.2x' % x for x in c[:16])
        raise IndexError("Cookie index out of range")

    def lexicon_size(self, field):
        size = (c_uint)()
        lib.tdb_lexicon_size(self._db, field, size)
        return size.value
