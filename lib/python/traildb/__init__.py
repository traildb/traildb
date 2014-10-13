import sys
import os
import string
from ctypes import CDLL, c_char_p, c_ubyte, POINTER, c_void_p, c_int, c_uint

cd = os.path.dirname(os.path.abspath(__file__))
lib = CDLL(os.path.join(cd, '_traildb.so'))

lib.tdb_get_cookie.argtypes = [c_void_p, c_uint]
lib.tdb_get_cookie.restype = POINTER(c_ubyte)

lib.tdb_num_cookies.argtypes = [c_void_p]
lib.tdb_num_cookies.restype = c_uint

lib.tdb_get_item_value.argtypes = [c_void_p, c_uint]
lib.tdb_get_item_value.restype = c_char_p

lib.tdb_decode_trail.argtypes = [c_void_p,
                                c_uint,
                                POINTER(c_uint),
                                c_uint,
                                c_uint]
lib.tdb_decode_trail.restype = c_uint

lib.tdb_open.argtypes = [c_char_p]
lib.tdb_open.restype = c_void_p

lib.tdb_lexicon_size.argtypes = [c_void_p, c_uint, POINTER(c_uint)]
lib.tdb_lexicon_size.restype = c_int

class TrailDB(object):

    TRAIL_SIZE_INCREMENT = 1000000

    def __init__(self, path):
        self._chunk = lib.tdb_open(path)
        self._max_idx = lib.tdb_num_cookies(self._chunk)
        self._fields = map(string.strip, open(os.path.join(path, 'fields')))
        self._trail_buf_size = 0
        self._grow_buffer()

    def _grow_buffer(self):
        self._trail_buf_size += self.TRAIL_SIZE_INCREMENT
        self._trail_buf = (c_uint * self._trail_buf_size)()

    def __len__(self):
        return self._max_idx

    def __getitem__(self, idx):
        return self.get_trail(idx)

    def get_trail(self, idx, lookup_values=True):
        while True:
            num = lib.tdb_decode_trail(self._chunk,
                                      idx,
                                      self._trail_buf,
                                      self._trail_buf_size,
                                      0)
            if num == 0:
                raise IndexError("Cookie index out of range")
            elif num == self._trail_buf_size:
                self._grow_buffer()
            else:
                buf = self._trail_buf
                break

        i = 0
        while i < num:
            tstamp = buf[i]
            i += 1
            fields = []
            while i < num and buf[i]:
                if lookup_values:
                    if buf[i] >> 8:
                        fields.append(lib.tdb_get_item_value(self._chunk, buf[i]))
                    else:
                        fields.append('')
                else:
                    if buf[i] >> 8:
                        fields.append(buf[i])
                    else:
                        fields.append(0)
                i += 1
            i += 1
            yield tstamp, fields

    def get_cookie(self, idx, raw_bytes=False):
        c = lib.tdb_get_cookie(self._chunk, idx)
        if c:
            if raw_bytes:
                return c[:16]
            else:
                return ''.join('%.2x' % x for x in c[:16])
        else:
            raise IndexError("Cookie index out of range")

    def get_item_value(self, field):
        return lib.tdb_get_item_value(self._chunk, field)

    def get_fields(self):
        return self._fields

    def get_lexicon_size(self, field):
        size = (c_uint)()
        lib.tdb_lexicon_size(self._chunk, field, size)
        return size.value
