#!/usr/bin/env python2

import sys, os
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def geomean(N):
	return reduce(lambda x,y: x*y, N)**(1.0/len(N))

LABELS = os.getenv('RCU_HTM_PLOT_LABELS', "").split()
EXECUTABLES = os.getenv('RCU_HTM_EXECUTABLES', "").split()
WORKLOADS = os.getenv('RCU_HTM_WORKLOADS', "").split()
INIT_SIZES = os.getenv('RCU_HTM_INIT_SIZES_LABELS', "").split()

COLORS = ["white","white","white","white","white","black","white"]
HATCHES = ['', '///', 'xxx', '\\\\\\', 'ooo', '', '---']

## Check that the appropriate env variables have been set
if (len(LABELS) == 0 or len(EXECUTABLES) == 0 or len(WORKLOADS) == 0 \
                     or len(INIT_SIZES) == 0):
	print "Make sure you have set the appropriate environment variables."
	print "Maybe you forgot to source the source_me.sh file"
	sys.exit(1)
## Sanity checks of the command line arguments provided
if (len(sys.argv) != 3):
	print "usage: " + sys.argv[0] + " <type> <input_file>"
	print "    type: can be one tree_size, workload, total"
	sys.exit(1)
stat_type = sys.argv[1]
input_file = sys.argv[2]
if (stat_type != "tree_size" and stat_type != "workload" and stat_type != "total"):
	print "Error: type must be one of tree_size, workload, total"
	sys.exit(1)

## Read the values from a file into a dictionary
d = dict()
fp = open(input_file)
line = fp.readline() ## The first line contains the number of threads
nthreads = int(line.split()[2])
line = fp.readline()
while line:
	[executable, init_size, workload, throughput] = line.split()

	if (stat_type == "total"):
		dict_key = executable
	elif (stat_type == "workload"):
		dict_key = executable + "_" + workload
	elif (stat_type == "tree_size"):
		dict_key = executable + "_" + init_size
	if not dict_key in d:
		d[dict_key] = []
	d[dict_key].append(float(throughput))

	line = fp.readline()

arrays_to_zip = []
if (stat_type == "total"):
	arr = []
	for exe in EXECUTABLES:
		key = exe
		arr.append(geomean(d[key]))
	arrays_to_zip.append(arr)
elif (stat_type == "workload"):
	for workload in WORKLOADS:
		arr = []
		for exe in EXECUTABLES:
			key = exe + "_" + workload
			arr.append(geomean(d[key]))
		arrays_to_zip.append(arr)
elif (stat_type == "tree_size"):
	for init_size in INIT_SIZES:
		arr = []
		for exe in EXECUTABLES:
			key = exe + "_" + init_size
			arr.append(geomean(d[key]))
		arrays_to_zip.append(arr)
			
zipped = zip(*arrays_to_zip)
ind = np.arange(len(zipped[0]))

if (stat_type == "tree_size"):
	plt.figure(num=0,figsize=(20,4))
elif (stat_type == "total"):
	plt.figure(num=0,figsize=(7,4))
elif (stat_type == "workload"):
	plt.figure(num=0,figsize=(20,4))

ax = plt.subplot(111)
width = 0.12
ax.yaxis.grid(True)
ax.set_ylabel("Speedup over serial")

if (stat_type == "tree_size"):
	ax.set_xlim(-0.2,len(INIT_SIZES))
	plt.xticks(np.arange(0.4,len(INIT_SIZES),1), INIT_SIZES)
elif (stat_type == "total"):
	ax.set_xlim(-0.1,0.9)
	plt.xticks((0.4,),(str(nthreads) + " Threads",))
elif (stat_type == "workload"):
	workload_labels = map(lambda x: x.split('_')[0]+"% lookups", WORKLOADS)
	ax.set_xlim(-0.2,len(WORKLOADS))
	plt.xticks(np.arange(0.4,len(WORKLOADS),1), workload_labels)

## Do the plotting
for i in range(len(zipped)):
	ax.bar(ind + i * width, zipped[i], width, color=COLORS[i], hatch=HATCHES[i], \
	       label = LABELS[i])

if (stat_type == "tree_size"):
	ax.set_aspect(0.05)
	ax.legend(ncol = 6, loc = "upper left", frameon=False, bbox_to_anchor=(0.0,-0.05))
elif (stat_type == "workload"):
	ax.set_aspect(0.05)

plt.title(str(nthreads) + " Threads")

plt.savefig("performance_evaluation_bars."+stat_type+"."+str(nthreads)+"_threads.png", bbox_inches = 'tight')
