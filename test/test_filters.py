from itertools import combinations, chain
from collections import Counter
import unittest
import string
import hashlib
import shutil

from traildb import TrailDBConstructor

class TestFilterSetter(unittest.TestCase):
    fields = ['a', 'b', 'c']

    def setUp(self):
        cookie = hashlib.md5('a').hexdigest()
        cons = TrailDBConstructor('test.tdb', self.fields)
        for i in range(100):
            cons.add(cookie,
                     123 + i,
                     ['%s%d' % (f, i) for f in self.fields])
        self.tdb = cons.finalize()

    def test_empty_filter(self):
        self.assertEquals(self.tdb.get_filter(), [])

    def test_simple_filter(self):
        q = [{'a': ['a1']}]
        self.tdb.set_filter(q)
        print self.assertEquals(self.tdb.get_filter(), q)

    def test_many_filters(self):
        for c in range(1, len(self.fields) + 1):
            for fields in combinations(self.fields, c):
                q = [{f: ['%s%d' % (f, i) for i in range(10)]}
                     for f in fields]
                q += [{f: [{'is_negative': True, 'value': '%s%d' % (f, i)}
                           for i in range(10)]}
                      for f in fields]
                self.tdb.set_filter(q)
                self.assertEquals(self.tdb.get_filter(), q)

    def tearDown(self):
        shutil.rmtree('test.tdb', True)

class TestFilterDecode(unittest.TestCase):
    fields = ['a', 'b', 'c']

    def setUp(self):
        self.stats = Counter()
        cons = TrailDBConstructor('test.tdb', self.fields)
        for cookie_id in range(100):
            cookie = hashlib.md5(str(cookie_id)).hexdigest()
            for i in range((cookie_id % 10) + 1):
                events = ['%s%d' % (f, i) for f in self.fields]
                cons.add(cookie, 123 + i, events)
                self.stats.update(events)

        self.tdb = cons.finalize()

    def test_one_term(self):
        for field_id, field in enumerate(self.fields):
            for i in range(10):
                key = '%s%d' % (field, i)
                self.tdb.set_filter([{field: [key]}])
                found = 0
                for cookie, trail in self.tdb.crumbs():
                    for event in trail:
                        self.assertEquals(event[field_id + 1], key)
                        found += 1
                self.assertEquals(found, self.stats[key])

    def test_one_negative_term(self):
        for field_id, field in enumerate(self.fields):
            for i in range(10):
                key = '%s%d' % (field, i)
                self.tdb.set_filter([{field: [{'is_negative': True,
                                               'value': key}]}])

                stats = Counter()
                for cookie, trail in self.tdb.crumbs():
                    for event in trail:
                        self.assertNotEquals(event[field_id + 1], key)
                        stats.update([event[field_id + 1]])

                correct = {f: v for f, v in self.stats.iteritems()
                           if f != key and f[0] == key[0]}
                self.assertEquals(stats, correct)

    def test_disjunction(self):
        for i in range(9):
            q = {f: ['%s%d' % (f, i), '%s%d' % (f, i + 1)]
                 for f in self.fields}
            self.tdb.set_filter([q])
            stats = Counter()
            for cookie, trail in self.tdb.crumbs():
                for event in trail:
                    for f, field in enumerate(self.fields):
                        self.assertTrue(event[f + 1] in q[field])
                        stats.update([event[f + 1]])
            for key in chain(*q.values()):
                self.assertEquals(stats[key], self.stats[key])

    def test_conjunction(self):
        for c in range(2, len(self.fields) + 1):
            for fields in combinations(self.fields, c):
                for i in range(10):
                    q = [{f: ['%s%d' % (f, i)]} for f in fields]
                    self.tdb.set_filter(q)
                    stats = Counter()
                    for cookie, trail in self.tdb.crumbs():
                        for event in trail:
                            for f, field in enumerate(self.fields):
                                self.assertTrue(event[f + 1], '%s%d' % (field, i))
                                stats.update([event[f + 1]])
                    for f in q:
                        key = f.values()[0][0]
                        print key, stats, q
                        self.assertEquals(stats[key], self.stats[key])

    def test_impossible_conjunction(self):
        for i in range(9):
            q = [{f: ['%s%d' % (f, i + j)]}
                 for j, f in enumerate(self.fields)]
            self.tdb.set_filter(q)
            impossible = True
            for cookie, trail in self.tdb.crumbs():
                for event in trail:
                    impossible = False
            self.assertTrue(impossible)

    def tearDown(self):
        shutil.rmtree('test.tdb', True)


#class TestFilterEdgeEncoded(unittest.TestCase):

if __name__ == '__main__':
    unittest.main()
