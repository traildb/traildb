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
                fields.append((fieldnames[j + 1], fieldval))
                offs += 4
            yield cookie, timestamp, fields

class Decoder(object):
    def __init__(self, blob):
        body_size, fields_size, lexicon_size = struct.unpack('QQQ', blob[:24])
        body_offs = 24
        fields_offs = body_offs + body_size
        lexicon_offs = fields_offs + fields_size

        self.fieldnames = list(decode_fields(blob[fields_offs:lexicon_offs]))
        self.lexicon = dict(decode_lexicon(blob[lexicon_offs:]))
        self.body = blob[body_offs:fields_offs]

    def __iter__(self):
        for cookie, timestamp, fields in decode_trails(self.body,
                                                       self.fieldnames,
                                                       self.lexicon):
            yield cookie, timestamp, fields

if __name__ == '__main__':
    for cookie, timestamp, fields in Decoder(open(sys.argv[1]).read()):
        print cookie, timestamp, ' | '.join('%s:%s' % x for x in fields)
