#!/usr/bin/env python
import tempfile
import shutil
import sys
import os

class Testing:
    def __init__(self):
        self.failed_tests = set()
        self.succeeded_tests = set()

    def runCTest(self, cfile, msg=''):
        (handle, path) = tempfile.mkstemp()
        temp_dir_path = tempfile.mkdtemp()
        try:
            os.close(handle)
            cmd = "cc %s -Wall --std=c99 -g3 %s -ltraildb -lJudy -o %s" %\
                  (os.getenv('CFLAGS', ''), cfile, path)
            if os.system(cmd) != 0:
                print("FAILED: %s" % cfile)
                self.failed_tests.add(cfile)
                return

            os.system("chmod +x %s" % path)
            result = os.system("%s \"%s\"" % (path, temp_dir_path))
            if result != 0:
                print("FAILED: %s %s" % (cfile, msg))
                self.failed_tests.add(cfile)
            else:
                print("SUCCEEDED: %s %s" % (cfile, msg))
                self.succeeded_tests.add(cfile)
        finally:
            try:
                os.remove(path)
            except:
                pass
            try:
                shutil.rmtree(temp_dir_path)
                os.remove(temp_dir_path + '.tdb')
            except:
                pass

    def runCTests(self, large_tests=False, package_tests=False, **kwargs):
        def walk(node):
            if os.path.isfile(node):
                if node.endswith(".c"):
                    self.runCTest(node)
                    if package_tests:
                        os.environ['TDB_CONS_OUTPUT_FORMAT'] = '1'
                        self.runCTest(node, '(packaged)')
                        del os.environ['TDB_CONS_OUTPUT_FORMAT']
            else:
                files = os.listdir(node)
                for f in files:
                    walk(os.path.join(node, f))

        walk("c-tests")
        if large_tests:
            walk("c-tests-large")

    def runTests(self, **kwargs):
        self.runCTests(**kwargs)

        if len(self.failed_tests) > 0:
            print("Tests that failed:")
            for s in self.failed_tests:
                print(s)

        print("Number of failed tests: %d" % len(self.failed_tests))
        print("Number of succeeded tests: %d" % len(self.succeeded_tests))
        if len(self.failed_tests) > 0:
            return -1

        return 0

if __name__ == '__main__':
    t = Testing()
    sys.exit(t.runTests(**{k: True for k in sys.argv[1:]}))
