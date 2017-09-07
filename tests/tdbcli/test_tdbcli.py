import glob
import os
import subprocess
import unittest
import json
import struct
import shutil
from itertools import imap
from collections import Counter
from collections import OrderedDict

TEST_DB = 'test_tdbcli_db'
ROOT = os.environ.get('TDBCLI_ROOT',
                      os.path.join(os.path.dirname(__file__), '../../build'))
TDB = os.path.join(ROOT, 'tdb')

class TdbCliTest(unittest.TestCase):

    def tdb_cmd(self, cmd, data=''):
        proc = subprocess.Popen(cmd,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                env={'LD_LIBRARY_PATH': ROOT,
                                     'DYLD_LIBRARY_PATH': ROOT},
                                bufsize=1024 * 1024)
        stdout, stderr = proc.communicate(data)
        if proc.returncode:
            raise Exception("tdb command '%s' failed (%d): %s" %\
                            (' '.join(cmd), proc.returncode, stderr))
        return stdout

    def _remove(self):
        for path in glob.glob('%s*' % TEST_DB):
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            except:
                pass

    def setUp(self):
        self._remove()
        return self.make_tdb()

    def make_tdb(self, events=None, suffix='', index=True):
        if events is None:
            events = list(self.events())

        fields = list(events[0])
        name = TEST_DB + suffix
        cmd = [TDB, 'make', '-j', '-o', name, '-f', ','.join(fields)]
        data = '\n'.join(imap(json.dumps, events)) + '\n'
        self.tdb_cmd(cmd, data)

        if index:
            cmd = [TDB, 'index', '-i', name]
            self.tdb_cmd(cmd)

        return name

    def dump(self, args=[], suffix=''):
        cmd = [TDB, 'dump', '-j', '-i', TEST_DB + suffix] + args
        return imap(json.loads, self.tdb_cmd(cmd).splitlines())

    def tearDown(self):
        self._remove()

    def hexuuid(self, i):
        return (struct.pack('Q', i) + '\x00' * 8).encode('hex')

    def assertFilter(self, filter_expr, expected, index_only=False, args=[]):
        if filter_expr:
            args += ['-F', filter_expr]
        if not index_only:
            self.assertEquals(list(self.dump(args + ['--no-index'])),
                              expected)
        self.assertEquals(list(self.dump(args)), expected)

class TestBasicDump(TdbCliTest):
    def events(self):
        for i in range(1000):
            yield {'uuid': self.hexuuid(i),
                   'time': str(i + 100),
                   'first': 'FF%dFF' % i,
                   'second': 'SS%dSS' % i}

    def test_basic(self):
        # assertEquals appears to be super slow with long lists
        self.assertTrue(list(self.dump()) == list(self.events()))

class TestBasicFilter(TdbCliTest):
    def events(self):
        for i in range(1000):
            yield {'uuid': self.hexuuid(i),
                   'time': str(i + 100),
                   'first': str(i),
                   'second': str(i % 10)}

    def test_single_item(self):
        events = list(self.events())
        for i in range(1000):
            self.assertFilter('first=%d' % i, [events[i]])

    def test_many_results(self):
        for i in range(10):
            events = [x for x in self.events() if x['second'] == str(i)]
            self.assertFilter('second=%d' % i, events)

    def test_basic_conjunction(self):
        events = [x for x in self.events()\
                  if x['second'] == '0' and x['first'] == '500']
        self.assertFilter('second=0 & first=500', events)

    def test_basic_disjunction(self):
        events = [x for x in self.events()\
                  if x['second'] == '0' or x['second'] == '9']
        self.assertFilter('second=0 second=9', events)

    def test_basic_negation(self):
        events = [x for x in self.events() if x['second'] != '0']
        self.assertFilter('second!=0', events)

    def test_nontrivial_filter(self):
        events = [x for x in self.events()\
                  if x['second'] != '0' and x['first'] in ('48', '469', '500')]
        self.assertFilter('second!=0 & first=48 first=469 first=500',
                          events)

    def test_uuid_filter(self):
        sample = (3, 23, 308, 940, 999)
        uuids = ','.join(self.hexuuid(i) for i in sample)
        events = [x for i, x in enumerate(self.events()) if i in sample]
        self.assertFilter(None, events, args=['--uuids', uuids])

    def test_uuid_and_event_filter(self):
        sample = (1, 8, 435, 123, 445, 446, 978)
        uuids = ','.join(self.hexuuid(i) for i in sample)
        events = [x for i, x in enumerate(self.events())\
                  if x['second'] != '0' and i in sample]
        self.assertFilter('second!=0', events, args=['--uuids', uuids])

# Multiple trails per page, num_trails > 2**16
class TestMediumFilter(TdbCliTest):
    def events(self):
        for i in range(100000):
            yield {'uuid': self.hexuuid(i),
                   'time': str(i + 100),
                   'first': str(i),
                   'second': str(i % 10)}

    def test_single_item(self):
        events = list(self.events())
        for i in range(500):
            self.assertFilter('first=%d' % i, [events[i]], index_only=True)
        for i in range(65000, 66000):
            self.assertFilter('first=%d' % i, [events[i]], index_only=True)
        for i in range(99000, 100000):
            self.assertFilter('first=%d' % i, [events[i]], index_only=True)

# Test overflow mapping when > 4 pages / item
class TestLargeFilter(TdbCliTest):
    def events(self):
        for i in range(1000000):
            yield {'uuid': self.hexuuid(i),
                   'time': str(i + 100),
                   'first': str(i),
                   'second': str(i % 10),
                   'third': 'const'}

    def test_single_item(self):
        events = list(self.events())
        for i in range(10):
            self.assertFilter('first=%d' % i, [events[i]], index_only=True)
        for i in range(999990, 1000000):
            self.assertFilter('first=%d' % i, [events[i]], index_only=True)

        self.assertFilter('third=const', events)
        self.assertFilter('third!=const', [])
        events = [x for x in self.events() if x['second'] == '5']
        self.assertFilter('second=5', events)
        events = [x for x in self.events()\
                  if x['second'] == '0' and x['first'] == '500']
        self.assertFilter('second=0 & first=500', events)

class TestMerge(TdbCliTest):
    def events_a(self):
        event_list = []
        for i in range(10):
            event_list.append(OrderedDict([('uuid', self.hexuuid(i)),
                                           ('time', str(i + 100)),
                                           ('alpha', chr(i + 65)),
                                           ('number', str(i))]))
        return event_list

    def events_b(self):
        """
        Same number of events and fields as 'a' but
        replace 'alpha' field with 'foobar' field
        and keep order of rest of fields.
        """
        event_list = []
        for i in range(10):
            event_list.append(OrderedDict([('uuid', self.hexuuid(i)),
                                           ('time', str(i + 100)),
                                           ('number', str(i)),
                                           ('foobar', str(i + 200))]))
        return event_list

    def events_c(self):
        """
        Same number of events as 'a' but add one more field.
        """
        event_list = []
        for i in range(10):
            event_list.append(OrderedDict([('uuid', self.hexuuid(i)),
                                           ('time', str(i + 100)),
                                           ('alpha', chr(i + 65)),
                                           ('number', str(i)),
                                           ('beta', str(75 - i))]))
        return event_list

    def events_d(self):
        """
        Same number of events and fields as 'a' but re-order fields.
        """
        event_list = []
        for i in range(10):
            event_list.append(OrderedDict([('uuid', self.hexuuid(i)),
                                           ('time', str(i + 100)),
                                           ('number', str(i)),
                                           ('alpha', chr(i + 65))]))
        return event_list                      

    def setUp(self):
        self._remove()

    def test_single_traildb(self):
        tdb_a = self.make_tdb(suffix='_a',
                              index=False,
                              events=self.events_a())
        cmd = [TDB, 'merge', '-o', TEST_DB + '_merged', tdb_a]
        self.tdb_cmd(cmd)
        stats = Counter(''.join(imap(str, sorted(e.items())))
                        for e in self.dump(suffix='_merged'))
        self.assertEquals(stats.values(), [1] * 10)

    def test_matching_fields(self):
        tdb_a = self.make_tdb(suffix='_a',
                              index=False,
                              events=self.events_a())
        tdb_b = self.make_tdb(suffix='_b',
                              index=False,
                              events=self.events_a())
        cmd = [TDB, 'merge', '-o', TEST_DB + '_merged', tdb_a, tdb_b]
        self.tdb_cmd(cmd)
        stats = Counter(''.join(imap(str, sorted(e.items())))
                        for e in self.dump(suffix='_merged'))
        self.assertEquals(stats.values(), [2] * 10)

    def test_reordered_fields(self):
        """
        Test case with same fields but re-ordered.
        """
        tdb_a = self.make_tdb(suffix='_a',
                              index=False,
                              events=self.events_a())
        tdb_b = self.make_tdb(suffix='_b',
                              index=False,
                              events=self.events_d())
        cmd = [TDB, 'merge', '-o', TEST_DB + '_merged', tdb_a, tdb_b]
        self.tdb_cmd(cmd)
        stats = Counter(''.join(imap(str, sorted(e.items())))
                        for e in self.dump(suffix='_merged'))
        self.assertEquals(stats.values(), [2] * 10)

    def test_additional_field(self):
        """
        Test case where one TDB has an additional field
        """
        tdb_a = self.make_tdb(suffix='_a',
                              index=False,
                              events=self.events_a())
        tdb_b = self.make_tdb(suffix='_b',
                              index=False,
                              events=self.events_c())
        cmd = [TDB, 'merge', '-o', TEST_DB + '_merged', tdb_a, tdb_b]
        self.tdb_cmd(cmd)
        all_fields = set()
        for e in self.dump(suffix='_merged'):
            all_fields.update(e.iterkeys())
        self.assertEquals(len(all_fields),
                          len(self.events_c()[0]))

    def test_mismatched_fields(self):
        """
        Test case with same number of fields but different
        fields and order
        """
        tdb_a = self.make_tdb(suffix='_a',
                              index=False,
                              events=self.events_a())
        tdb_b = self.make_tdb(suffix='_b',
                              index=False,
                              events=self.events_b())
        cmd = [TDB, 'merge', '-o', TEST_DB + '_merged', tdb_a, tdb_b]
        self.tdb_cmd(cmd)
        numbersum = 0
        for ev in self.dump(suffix='_merged'):
            self.assertEquals(len(ev), 5)
            if ev['alpha']:
                self.assertEquals(ev['foobar'], '')
            if ev['foobar']:
                self.assertEquals(ev['alpha'], '')
            numbersum += int(ev['number'])
        self.assertEquals(numbersum, 90)

    def test_merge_with_uuids(self):
        tdb_a = self.make_tdb(suffix='_a',
                              index=False,
                              events=self.events_a())
        tdb_b = self.make_tdb(suffix='_b',
                              index=False,
                              events=self.events_a())
        uuids = [e['uuid'] for e in self.events_a()][:2]
        cmd = [TDB, 'merge', '-o', TEST_DB + '_merged',
               '--uuids', ','.join(uuids), tdb_a, tdb_b]
        self.tdb_cmd(cmd)
        ev = list(self.dump(suffix='_merged'))
        stats = Counter(''.join(imap(str, sorted(e.items()))) for e in ev)
        self.assertEquals(stats.values(), [2] * 2)
        self.assertEquals({e['uuid'] for e in ev}, set(uuids))

if __name__ == '__main__':
    unittest.main()
