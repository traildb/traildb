#!/usr/bin/env python
import tempfile
import shutil
import os

class Testing:
    def __init__(self):
        self.failed_tests = { }
        self.succeeded_tests = { }

    def runCTest(self, cfile):
        (handle, path) = tempfile.mkstemp()
        temp_dir_path = tempfile.mkdtemp()
        try:
            os.close(handle)
            if os.system("cc -Wall --std=c99 -g3 %s -ltraildb -o %s" % (cfile, path)) != 0:
                print("FAILED: %s" % cfile)
                self.failed_tests[cfile] = 1
                return

            os.system("chmod +x %s" % path)
            result = os.system("%s \"%s\"" % (path, temp_dir_path))
            if result != 0:
                print("FAILED: %s" % cfile)
                self.failed_tests[cfile] = 1
            else:
                print("SUCCEEDED: %s" % cfile)
                self.succeeded_tests[cfile] = 1
        finally:
            try:
                os.remove(path)
            except:
                pass
            try:
                shutil.rmtree(temp_dir_path)
            except:
                pass

    def runCTests(self):
        def walk(node):
            if os.path.isfile(node):
                if node.endswith(".c"):
                    self.runCTest(node)
            else:
                files = os.listdir(node)
                for f in files:
                    walk(os.path.join(node, f))

        return walk("c-tests")

    def runTests(self):
        self.runCTests()
        print("Number of failed tests: %d" % len(self.failed_tests))
        print("Number of succeeded tests: %d" % len(self.succeeded_tests))

if __name__ == '__main__':
    t = Testing()
    t.runTests()

