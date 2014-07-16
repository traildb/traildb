import sys
import struct

# Blob is structured in four distinct sections:
#
# HEADER  [ body offset | fields offset | lexicon offset ]
# BODY    [ trails data ]
# FIELDS  [ list of field names ]
# LEXICON [ field - field value pairs ]
#

cookie = struct.Struct('B' * 16)
value = struct.Struct('I')

def decode_string(data, offs):
    size = value.unpack(data[offs:offs + 4])[0]
    offs += 4
    return offs + size, data[offs:offs + size]

def decode_fields(data):
    offs = 0
    while offs < len(data):
        offs, fieldname = decode_string(data, offs)
        yield fieldname

def decode_lexicon(data):
    offs = 0
    while offs < len(data):
        field = value.unpack(data[offs:offs + 4])[0]
        offs, fieldvalue = decode_string(data, offs + 4)
        yield field, fieldvalue

def decode_trails(data, fieldnames, lexicon):
    offs = 0
    while offs < len(data):
        # A cookie line is structured as follows:
        # COOKIE: 16-byte cookie
        # NUMBER-OF-LOGLINES: 4-byte uint
        # LOGLINES:
        #   TIMESTAMP: 4-byte uint
        #   FIELDS: A fixed-length set of fields (len(fieldnames) - 1)
        #     FIELD: 4-byte uint, maps to lexicon
        cookie = ''.join('%.2x' % ord(x) for x in data[offs:offs + 16])
        offs += 16
        num_loglines = value.unpack(data[offs:offs + 4])[0]
        offs += 4
        for i in range(num_loglines):
            timestamp = value.unpack(data[offs:offs + 4])[0]
            offs += 4
            fields = []
            for j in range(len(fieldnames) - 1):
                field = value.unpack(data[offs:offs + 4])[0]
                fieldval = '' if field == 0 else lexicon[field]
                fields.append('%s:%s' % (fieldnames[j + 1], fieldval))
                offs += 4
            yield cookie, timestamp, fields

def decode(blob):
    body_offs, fields_offs, lexicon_offs = struct.unpack('III', blob[:12])
    fieldnames = list(decode_fields(blob[fields_offs:lexicon_offs]))
    lexicon = dict(decode_lexicon(blob[lexicon_offs:]))
    body = blob[body_offs:fields_offs]
    for cookie, timestamp, fields in decode_trails(body, fieldnames, lexicon):
        print cookie, timestamp, ' | '.join(fields)

if __name__ == '__main__':
    decode(open(sys.argv[1]).read())
