from itertools import imap
import ctypes

arr = (ctypes.c_uint * 1000)()

def cookie_index(cookie, chunk):
    cookie = [int(cookie[i:i+2], 16) for i in range(0, 32, 2)]
    for i in range(bd.bd_num_cookies(chunk)):
        c = bd.bd_lookup_cookie(chunk, i)
        for j in range(16):
            if c[j] != cookie[j]:
                break
        else:
            return i

def cookie_string(idx):
    c = bd.bd_lookup_cookie(chunk, idx)[:16]
    return ''.join(imap(lambda x: '%.2x' % x, c))

def get_trail(idx):
    le = bd.bd_trail_decode(chunk, idx, arr, 1000)
    i = 0
    while i < le:
        tstamp = arr[i]
        i += 1
        fields = []
        while i < le and arr[i]:
            if arr[i] >> 8:
                fields.append(bd.bd_lookup_value(chunk, arr[i]))
            else:
                fields.append('')
            i += 1
        i += 1
        yield tstamp, fields

bd = ctypes.CDLL('breadcrumbs.so')
bd.bd_error.restype = ctypes.c_char_p
bd.bd_lookup_cookie.restype = ctypes.POINTER(ctypes.c_ubyte)
bd.bd_num_cookies.restype = ctypes.c_uint
bd.bd_lookup_value.restype = ctypes.c_char_p
bd.bd_trail_decode.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.POINTER(ctypes.c_uint), ctypes.c_uint]

chunk = bd.bd_open('out')
#idx = cookie_index('00047f2de1a1c57840b5f8968964b9c1', chunk)

for idx in range(bd.bd_num_cookies(chunk)):
    cookie = cookie_string(idx)
    for tstamp, fields in get_trail(idx):
        print cookie, tstamp, ' '.join(fields)

