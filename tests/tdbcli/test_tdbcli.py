import os
import subprocess
import unittest
import json
import struct
import shutil
from itertools import imap

TEST_DB = 'test_tdbcli_db'
ROOT = os.environ.get('TDBCLI_ROOT',
                      os.path.join(os.path.dirname(__file__), '../../build'))
TDB = os.path.join(ROOT, 'tdb')

class TdbCliTest(unittest.TestCase):

    def _run(self, cmd, data=''):
        proc = subprocess.Popen(cmd,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                env={'LD_LIBRARY_PATH': ROOT},
                                bufsize=1024 * 1024)
        stdout, stderr = proc.communicate(data)
        if proc.returncode:
            raise Exception("tdb command '%s' failed (%d): %s" %\
                            (' '.join(cmd), proc.returncode, stderr))
        return stdout

    def _remove(self):
        try:
            os.remove('%s.tdb' % TEST_DB)
            os.remove('%s.tdb.index' % TEST_DB)
            shutil.rmtree(TEST_DB)
        except:
            pass

    def setUp(self):
        self._remove()
        events = list(self.events())
        fields = list(events[0])
        cmd = [TDB, 'make', '-j', '-o', TEST_DB, '-f', ','.join(fields)]
        data = '\n'.join(imap(json.dumps, events)) + '\n'
        self._run(cmd, data)
        cmd = [TDB, 'index', '-i', TEST_DB]
        self._run(cmd)

    def dump(self, args=[]):
        cmd = [TDB, 'dump', '-j', '-i', TEST_DB] + args
        return imap(json.loads, self._run(cmd).splitlines())

    def tearDown(self):
        self._remove()

    def hexuuid(self, i):
        return (struct.pack('Q', i) + '\x00' * 8).encode('hex')

    def assertFilter(self, filter_expr, expected, index_only=False):
        args = ['-F', filter_expr]
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


if __name__ == '__main__':
    unittest.main()
