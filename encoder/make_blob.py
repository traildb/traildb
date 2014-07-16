import sys
import random
import cStringIO
import struct

import breadcrumbs

cookie = struct.Struct('B' * 16)
value = struct.Struct('I')

def encode_trails(bc, sample_size, out):
    fieldset = set()
    for idx in random.sample(xrange(len(bc)), sample_size):
        out.write(cookie.pack(*bc.get_cookie(idx, raw_bytes=True)))
        trail = list(bc.get_trail(idx, lookup_values=False))
        out.write(value.pack(len(trail)))
        for timestamp, fields in trail:
            out.write(value.pack(timestamp))
            for field in fields:
                out.write(value.pack(field))
                if field:
                    fieldset.add(field)
    return fieldset

def encode_lexicon(bc, fieldset, out):
    for field in fieldset:
        s = bc.lookup_value(field)
        out.write(value.pack(field))
        out.write(value.pack(len(s)))
        out.write(s)

def encode_fields(bc, out):
    for fieldname in bc.get_fields():
        out.write(value.pack(len(fieldname)))
        out.write(fieldname)

def dump_blob(sections, out):
    offs = 12
    for sect in sections:
        out.write(value.pack(offs))
        offs += len(sect)
    for sect in sections:
        out.write(sect)

def main():
    bc = breadcrumbs.Breadcrumb(sys.argv[1])
    sample_size = int(sys.argv[2])
    blob = open(sys.argv[3], 'w')

    body = cStringIO.StringIO()
    fieldset = encode_trails(bc, sample_size, body)
    lexicon = cStringIO.StringIO()
    encode_lexicon(bc, fieldset, lexicon)
    fields = cStringIO.StringIO()
    encode_fields(bc, fields)

    dump_blob([s.getvalue() for s in (body, fields, lexicon)], blob)
    blob.close()

main()
