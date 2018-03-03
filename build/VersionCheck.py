#!/usr/bin/python

#
# VersionCheck.py:
#   Execute the given command, which must output a version of the form:
#
#   <Major>{.<Minor>.<Patch>}, where all three fields are non-negative integers and missing components default to 0
#
#   and check against the supplied minimum version components.
#
#   Returns 1 if the version is at least the minimum, 0 if not, or else -1 if an error occurs.
#

import os, sys

def VersionCheck(command, minVersion):
    try:
        minVers = minVersion.split('.')
        while (len(minVers) < 3):
            minVers.append(0)
        minMajor, minMinor, minPatch  = [int(c) for c in minVers]
        vers = os.popen(command).read().strip().split('.')
        while (len(vers) < 3):
            vers.append(0)
        major, minor, patch = [int(c) for c in vers]
        return 1 if (major > minMajor or
                     (major == minMajor and (minor > minMinor or
                                             (minor == minMinor and patch >= minPatch)))) else 0
    except:
        return -1

sys.stdout.write(str(VersionCheck(*sys.argv[1:3])))
