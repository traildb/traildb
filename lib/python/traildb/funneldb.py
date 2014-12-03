import os

from ctypes import c_char, c_char_p, c_ubyte, c_void_p, c_int
from ctypes import c_uint, c_uint8, c_uint32, c_uint64
from ctypes import CDLL, POINTER, Structure, Union, byref, cast, pointer
from itertools import product
from operator import __and__, __or__

from .traildb import tdb, tdb_field, tdb_item, tdb_fold_fn

cd = os.path.dirname(os.path.abspath(__file__))
lib = CDLL(os.path.join(cd, 'libtraildb.so'))

def api(fun, args, res=None):
    fun.argtypes = args
    fun.restype = res

def struct(fields):
    class struct(Structure):
        _fields_ = fields
    return struct

def union(fields):
    class union(Union):
        _fields_ = fields
    return union

FDB_DENSE   = 1
FDB_SPARSE  = 2

FDB_SIMPLE  = 1
FDB_COMPLEX = 2

fdb_cons = c_void_p
fdb_eid  = c_uint32
fdb_fid  = c_uint32
fdb_mask = c_uint8

FDB = struct([('data_offs', c_uint64),
              ('data_size', c_uint64),
              ('params', c_uint8 * 1024),
              ('num_funnels', fdb_fid)])
fdb = POINTER(FDB)

FDB_EZ = struct([('num_keys', c_uint),
                 ('key_offs', fdb_fid * 128),
                 ('key_fields', tdb_field * 128),
                 ('mask_field', tdb_field)])
fdb_ez = POINTER(FDB_EZ)

FDB_ELEM = struct([('id', fdb_eid),
                   ('mask', fdb_mask)])
fdb_elem = POINTER(FDB_ELEM)

FDB_CLAUSE = struct([('terms', c_uint64),
                     ('nterms', c_uint64)])
fdb_clause = POINTER(FDB_CLAUSE)

FDB_CNF = struct([('num_clauses', c_uint),
                  ('clauses', fdb_clause)])
fdb_cnf = POINTER(FDB_CNF)

class FDB_SET(Structure):
    pass
fdb_set = POINTER(FDB_SET)

FDB_SET_SIMPLE = struct([('db', fdb),
                         ('funnel_id', fdb_fid),
                         ('cnf', fdb_cnf)])
fdb_set_simple = POINTER(FDB_SET_SIMPLE)

FDB_SET_COMPLEX = struct([('db', fdb),
                          ('num_sets', c_uint),
                          ('sets', fdb_set),
                          ('cnf', fdb_cnf)])
fdb_set_complex = POINTER(FDB_SET_SIMPLE)

FDB_SET._anonymous_ = ['_impl']
FDB_SET._fields_ = [('flags', c_uint8),
                    ('_impl', union([('simple', FDB_SET_SIMPLE),
                                     ('complex', FDB_SET_COMPLEX)]))]

FDB_FAMILY = struct([('db', fdb),
                     ('num_sets', c_uint),
                     ('funnel_id', fdb_fid),
                     ('cnfs', fdb_cnf)])
fdb_family = POINTER(FDB_FAMILY)

fdb_iter = c_void_p

api(lib.fdb_create, [tdb, tdb_fold_fn, fdb_fid, c_void_p], fdb)
api(lib.fdb_detect, [fdb_fid, fdb_eid, fdb_mask, fdb_cons])

api(lib.fdb_easy, [tdb, fdb_ez], fdb)
api(lib.fdb_dump, [fdb, c_int], fdb)
api(lib.fdb_load, [c_int], fdb)
api(lib.fdb_free, [fdb], fdb)

api(lib.fdb_iter_new, [fdb_set], fdb_iter);
api(lib.fdb_iter_next, [fdb_iter], fdb_elem);
api(lib.fdb_iter_free, [fdb_iter], fdb_iter);

api(lib.fdb_count_set, [fdb_set, POINTER(fdb_eid)], c_int)
api(lib.fdb_count_family, [fdb_family, POINTER(fdb_eid)], c_int)

def keys(num_keys, key_fields):
    key = []
    for i, f in enumerate(key_fields):
        if not num_keys:
            return
        if f:
            key.append(f)
        else:
            num_keys -= 1
            yield tuple(sorted(key)), i - len(key)
            key = []

def seqify(x):
    try:
        x[0]
        return x
    except IndexError:
        return x
    except TypeError:
        return x,

def split(terms, n):
    buckets = [[] for i in xrange(n)]
    for i, term in enumerate(terms):
        buckets[i % n].append(term)
    return buckets

class FunnelDBError(Exception):
    pass

class FunnelDB(object):
    def __init__(self, traildb, db):
        self.db = db
        self.traildb = traildb
        self.num_funnels = db.contents.num_funnels
        self.params = cast(db.contents.params, fdb_ez).contents
        self.keydict = dict(keys(self.params.num_keys, self.params.key_fields))
        self.mask = traildb.fields[self.params.mask_field]

    def __del__(self):
        lib.fdb_free(self.db)

    def __len__(self):
        return self.num_funnels

    def __getitem__(self, id):
        return self.funnel(id)

    def funnel(self, id, cnf=None):
        if id < 0 or id >= len(self):
            raise IndexError("Invalid funnel id: %s" % id)
        items = FDB_SET_SIMPLE(db=self.db, funnel_id=id, cnf=cnf)
        index = fdb_eid()
        next_ = FDB_ELEM()
        while True:
            if not lib.fdb_next(items, byref(index), byref(next_)):
                return
            yield next_.id, next_.mask

    def key(self, **which):
        tdb = self.traildb
        O, F = self.params.key_offs, self.params.key_fields
        which = dict((tdb.fields.index(k), v) for k, v in which.items())
        index = self.keydict[tuple(sorted(which))]
        key = O[index - 1] if index else 0
        for i, f in enumerate(F[index:]):
            if not f:
                break
            key += tdb.val(f, which[f]) * O[index + i]
        return key

    def keys(self):
        tdb = self.traildb
        for _, key in sorted((v, k) for k, v in self.keydict.items()):
            names = [tdb.fields[f] for f in key]
            for which in product(*(tdb.lexicon(f) for f in key)):
                yield dict(zip(names, which))

    def path_key(self, path):
        return self.key(**dict(i.split(':') for i in path.split('/') if i))

    def simple_cnfs(self, mask_filters):
        if isinstance(mask_filters[0], FDB_CNF):
            return mask_filters
        f = self.params.mask_field
        tdb = self.traildb
        cnfs = (FDB_CNF * len(mask_filters))()
        for i, mask_filter in enumerate(mask_filters):
            if mask_filter and mask_filter != '*':
                q = Q.parse(mask_filter, ext=True)
                cnfs[i].num_clauses = len(q.clauses)
                cnfs[i].clauses = (FDB_CLAUSE * len(q.clauses))()
                for n, c in enumerate(q.clauses):
                    for l in c.literals:
                        if l.negated:
                            cnfs[i].clauses[n].nterms |= 1 << tdb.val(f, l.term)
                        else:
                            cnfs[i].clauses[n].terms |= 1 << tdb.val(f, l.term)
            else:
                cnfs[i].num_clauses = 0
        return cnfs

    def fdb_sets(self, terms, sets=None):
        sets = sets or (FDB_SET * len(terms))()
        for i, term in enumerate(terms):
            if isinstance(term, FDB_SET):
                sets[i] = term
            elif isinstance(term, fdb_set):
                sets[i] = term.contents
            else:
                if isinstance(term, basestring):
                    path, mask_filter = term.split('=')
                    funnel_id = self.path_key(path)
                else:
                    funnel_id, mask_filter = term
                sets[i].flags = FDB_SIMPLE
                sets[i].simple.db = self.db
                sets[i].simple.funnel_id = funnel_id
                sets[i].simple.cnf = self.simple_cnfs([mask_filter])
        return sets

    def complex_set(self, query):
        q = Q.parse(query)
        terms = list(set(l.term for c in q.clauses for l in c.literals))
        cset = FDB_SET()
        cnf = FDB_CNF()
        cset.flags = FDB_COMPLEX
        cset.complex.db = self.db
        cset.complex.num_sets = len(terms)
        cset.complex.sets = self.fdb_sets(terms)
        cset.complex.cnf = pointer(cnf)
        cnf.num_clauses = len(q.clauses)
        cnf.clauses = (FDB_CLAUSE * len(q.clauses))()
        for n, c in enumerate(q.clauses):
            for l in c.literals:
                if l.negated:
                    cnf.clauses[n].nterms |= 1 << terms.index(l.term)
                else:
                    cnf.clauses[n].terms |= 1 << terms.index(l.term)
        return pointer(cset)

    def count_set(self, set):
        count = fdb_eid()
        if isinstance(set, fdb_set):
            lib.fdb_count_set(set, pointer(count))
        elif isinstance(set, FDB_SET):
            lib.fdb_count_set(pointer(set), pointer(count))
        else:
            lib.fdb_count_set(self.complex_set(set), pointer(count))
        return count.value

    def count_families(self, keys, mask_filters=None):
        fmasks = seqify(mask_filters)
        family = FDB_FAMILY(db=self.db)
        family.num_sets = len(fmasks)
        family.cnfs = self.simple_cnfs(fmasks)
        counts = (fdb_eid * family.num_sets)()
        for key in seqify(keys):
            family.funnel_id = key
            lib.fdb_count_family(family, counts)
            yield key, zip(fmasks, counts)

    def conjunction(self, terms, cset=None, N=64):
        cset = cset or FDB_SET()
        cset.flags = FDB_COMPLEX
        cset.complex.db = self.db
        cnf = FDB_CNF()
        cnf.num_clauses = N
        cnf.clauses = (FDB_CLAUSE * N)()
        for i in xrange(N):
            cnf.clauses[i].terms = 1 << i
        cset.complex.cnf = pointer(cnf)
        if len(terms) <= N:
            cset.complex.num_sets = len(terms)
            cset.complex.sets = self.fdb_sets(terms)
        else:
            cset.complex.num_sets = N
            cset.complex.sets = (FDB_SET * N)()
            for i, bucket in enumerate(split(terms, n=N)):
                self.conjunction(bucket, cset.complex.sets[i])
        return cset

    def disjunction(self, terms, cset=None, N=64):
        cset = cset or FDB_SET()
        cset.flags = FDB_COMPLEX
        cset.complex.db = self.db
        cnf = FDB_CNF()
        cnf.num_clauses = 1
        cnf.clauses = (FDB_CLAUSE * 1)()
        cnf.clauses[0].terms = (1 << N) - 1
        cset.complex.cnf = pointer(cnf)
        if len(terms) <= N:
            cset.complex.num_sets = len(terms)
            cset.complex.sets = self.fdb_sets(terms)
        else:
            cset.complex.num_sets = N
            cset.complex.sets = (FDB_SET * N)()
            for i, bucket in enumerate(split(terms, n=N)):
                self.disjunction(bucket, cset.complex.sets[i])
        return cset

    def difference(self, include, exclude):
        cset = FDB_SET()
        cset.flags = FDB_COMPLEX
        cset.complex.db = self.db
        cset.complex.num_sets = 2
        cset.complex.sets = (FDB_SET * 2)()
        cset.complex.sets[0] = include
        cset.complex.sets[1] = exclude
        cnf = FDB_CNF()
        cnf.num_clauses = 2
        cnf.clauses = (FDB_CLAUSE * 2)()
        cnf.clauses[0].terms = 1
        cnf.clauses[1].nterms = 2
        cset.complex.cnf = pointer(cnf)
        return cset

    @classmethod
    def easy(cls, traildb, keys=((),), mask_field=1):
        params = FDB_EZ(num_keys=len(keys), mask_field=mask_field)
        i = 0
        for group in keys:
            for k in seqify(group):
                params.key_fields[i] = traildb.fields.index(k)
                i += 1
            i += 1
        return cls(traildb, lib.fdb_easy(traildb._db, params))

    @classmethod
    def load(cls, traildb, path):
        with open(path) as file:
            db = lib.fdb_load(file.fileno())
        if not db:
            raise FunnelDBError("Could not open %s" % path)
        return cls(traildb, db)

    def dump(self, path):
        with open(path, 'w') as file:
            lib.fdb_dump(self.db, file.fileno())

class Q(object):
    def __init__(self, clauses):
        self.clauses = frozenset(c for c in clauses if c.literals)

    def __and__(self, other):
        return Q(self.clauses | other.clauses)

    def __or__(self, other):
        if not self.clauses:
            return other
        if not other.clauses:
            return self
        return Q(c | d for c in self.clauses for d in other.clauses)

    def __invert__(self):
        if not self.clauses:
            return self
        return Q.wrap(reduce(__or__, (~c for c in self.clauses)))

    def __cmp__(self, other):
        return cmp(str(self), str(other))

    def __eq__(self, other):
        return self.clauses == other.clauses

    def __hash__(self):
        return hash(self.clauses)

    def __str__(self):
        format = '(%s)' if len(self.clauses) > 1 else '%s'
        return ','.join(format % c for c in self.clauses)

    @classmethod
    def parse(cls, string, ext=False):
        import re
        if isinstance(string, cls):
            return string
        if ext:
            string = string.replace('!', '~').replace(',', '&').replace('+', '|')
        return eval(re.sub(r'([^&|~()\s][^&|~()]*)',
                           r'Q.wrap("""\1""".strip())', string) or 'Q([])')

    @classmethod
    def wrap(cls, proposition):
        if isinstance(proposition, Q):
            return proposition
        if isinstance(proposition, Clause):
            return Q((proposition, ))
        if isinstance(proposition, Literal):
            return Q((Clause((proposition, )), ))
        return Q((Clause((Literal(proposition), )), ))

class Clause(object):
    def __init__(self, literals):
        self.literals = frozenset(literals)

    def __and__(self, other):
        return Q.wrap(self) & Q.wrap(other)

    def __or__(self, other):
        return Clause(self.literals | other.literals)

    def __invert__(self):
        return Q(Clause([~l]) for l in self.literals)

    def __eq__(self, other):
        return self.literals == other.literals

    def __hash__(self):
        return hash(self.literals)

    def __str__(self):
        return '+'.join('%s' % l for l in self.literals)

class Literal(object):
    def __init__(self, term, negated=False):
        self.term    = term
        self.negated = negated

    def __and__(self, other):
        return Clause((self, )) & Clause((other, ))

    def __or__(self, other):
        return Clause((self, other))

    def __invert__(self):
        return type(self)(self.term, negated=not self.negated)

    def __eq__(self, other):
        return self.term == other.term and self.negated == other.negated

    def __hash__(self):
        return hash(self.term) ^ hash(self.negated)

    def __str__(self):
        return '%s%s' % ('!' if self.negated else '', self.term)
