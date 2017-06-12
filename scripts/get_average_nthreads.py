#!/usr/bin/env python

import sys

if (len(sys.argv) <= 1):
	print "usage: " + sys.argv[0] + " file1 file2 ... fileN"
	sys.exit(1)

def avg(a):
	return sum(a) / len(a)

d = dict() # [ nthreads: [val1, val2, ...], ... ]

for filename in sys.argv[1:]:
	fp = open(filename, "r")

	line = fp.readline()
	while line:
		tokens = line.split()

		if line.startswith("  num_threads:"):
			nthreads = int(tokens[1])
			if not nthreads in d:
				d[nthreads] = []
		elif line.startswith("Throughput(Ops/usec):"):
			throughput = float(tokens[1])
			d[nthreads].append(throughput)

		line = fp.readline()

keys = d.keys()
keys.sort()
for nthreads in keys:
	print "nthreads " + str(nthreads) + ":", avg(d[nthreads])
