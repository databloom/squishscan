#!/usr/local/bin/python

import magic

designation = magic.from_buffer(open("testdata/test.txt.gz").read(1024))

print("here's my",designation)
