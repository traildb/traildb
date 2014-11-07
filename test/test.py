import shutil
import subprocess
import unittest

from traildb import TrailDB, TrailDBConstructor

class TestAPI(unittest.TestCase):
    def setUp(self):
        subprocess.Popen(('test/test.sh', 'test.tdb')).wait()
        self.traildb = TrailDB('test.tdb')

    def tearDown(self):
        shutil.rmtree('test.tdb')

    def test_trails(self):
        db = self.traildb
        print list(db.trail(0, ptime=True))
        print list(db[0])

        for trail in db:
            for event in trail:
                print event.time, event.z

        for cookie, trail in db.crumbs():
            print cookie, len(list(trail))

    def test_fields(self):
        db = self.traildb
        print db.fields

    def test_cookies(self):
        db = self.traildb
        print db.cookie(0)
        print db.has_cookie_index()
        print db.cookie_id('12345678123456781234567812345678')
        #print db.cookie_id('abc')
        #print 'abc' in db

    def test_values(self):
        db = self.traildb
        print db.value(1, 1)

    def test_lexicons(self):
        db = self.traildb
        print db.lexicon_size(1)
        print db.lexicon(1)
        print db.lexicon('z')
        print dict((f, db.lexicon(f)) for f in db.fields[1:])

    def test_metadata(self):
        db = self.traildb
        print db.time_range()
        print db.time_range(ptime=True)

    def test_fold(self):
        db = self.traildb
        def fold_fn(db, id, ev, acc):
            acc.append((id, ev))
        print db.fold(fold_fn, [])

class TestCons(unittest.TestCase):
    def test_cons(self):
        cons = TrailDBConstructor('test.tdb', ['field1', 'field2'])
        cons.add('12345678123456781234567812345678', 123, ['a'])
        cons.add('12345678123456781234567812345678', 124, ['b', 'c'])
        tdb = cons.finalize()
        for cookie, trail in tdb.crumbs():
            print cookie, list(trail)
        print tdb.cookie_id('12345678123456781234567812345678')
        print tdb.cookie(0)

    def test_append(self):
        cons = TrailDBConstructor('test.tdb', ['field1', 'field2'])
        cons.add('12345678123456781234567812345678', 123, ['a'])
        cons.add('12345678123456781234567812345678', 124, ['b', 'c'])
        tdb = cons.finalize()
        cons = TrailDBConstructor('test2.tdb', ['field1', 'field2'])
        cons.add('12345678123456781234567812345678', 125, ['a'])
        cons.append(tdb)
        tdb = cons.finalize()
        for cookie, trail in tdb.crumbs():
            print cookie, list(trail)
        print tdb.cookie_id('12345678123456781234567812345678')
        print tdb.cookie(0)

    def tearDown(self):
        shutil.rmtree('test.tdb', True)
        shutil.rmtree('test2.tdb', True)

if __name__ == '__main__':
    unittest.main()