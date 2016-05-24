#!/usr/bin/env python
import os
import subprocess
import signal
import time
import sys

oldcwd = os.getcwd()

tester = None

def main():
    # add source directory
    global tester
    old_cflags = os.getenv("CFLAGS", "")
    build_dir = oldcwd + "/build"
    include = oldcwd + "/src"

    cflags = "%s -L %s -I%s" % (old_cflags, build_dir, include)

    os.putenv("CFLAGS", cflags)
    os.putenv("LD_LIBRARY_PATH", build_dir)

    os.chdir("./tests")

    tester = subprocess.Popen(["./support/test.py"])
    if tester.wait() != 0:
        sys.exit(1)

def stop_tester(signal, frame):
    if tester is not None:
        for x in range(4):
            if tester.returncode is None:
                tester.terminate()
                time.sleep(1)

    if tester.returncode is None:
        tester.kill()

    sys.exit(2)

if __name__ == "__main__":
    print "Run ./waf install --test_build before running this!"
    signal.signal(signal.SIGINT, stop_tester)
    main()
