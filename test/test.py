import shutil
import subprocess
import unittest

from traildb import TrailDB

class TestAPI(unittest.TestCase):
    def setUp(self):
        subprocess.Popen(('test/test.sh', 'test.tdb')).wait()
        self.traildb = TrailDB('test.tdb') # XXX: need encode api!

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
        print dict((f, db.lexicon(f)) for f in db.fields)

    def test_metadata(self):
        db = self.traildb
        print db.time_range()
        print db.time_range(ptime=True)

if __name__ == '__main__':
    unittest.main()