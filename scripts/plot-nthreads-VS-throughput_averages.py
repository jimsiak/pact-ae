#!/usr/bin/env python

import sys, os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

def prefix_large_number(number, divider = 1000):
	prefixes = [ "", "K", "M", "G" ]
	prefix_index = 0
#	divider = 1000

	while (number / divider >= 1):
		if (number % divider):
			print "Wrong number given for prefix " + str(number)
			sys.exit(1)

		prefix_index += 1
		number = number / divider

	return str(number) + prefixes[prefix_index]

def get_average(array_of_arrays):
	array_of_sums = np.array(array_of_arrays[0]);
	for array in array_of_arrays[1:]:
		array_of_sums = array_of_sums + np.array(array)

	array_of_avgs = array_of_sums / len(array_of_arrays)
	return array_of_avgs

def get_label_from_filename(filename):
	if "x.avl.bronson" in filename:
		lab = "bronson-avl"
		marker = 's'
		line_style = '-'
		line_color = 'purple'
	elif "x.bst.aravind" in filename:
		lab = "natarajan-bst"
		marker = 'D'
		line_style = '-'
		line_color = 'gray'
	elif "x.bst.citrus" in filename:
		lab = "citrus-bst"
		marker = '^'
		line_style = '-'
		line_color = 'blue'
	elif "x.avl.int.rcu_sgl" in filename:
		lab = "rcu-sgl-avl"
		marker = '^'
		line_style = '-'
		line_color = 'yellow'
	elif "x.avl.int.cop" in filename:
		lab = "cop-avl"
		marker = 'o'
		line_style = '-'
		line_color = 'red'
	elif "x.avl.int.rcu_htm" in filename:
		lab = "rcu-htm-avl"
		marker = 'x'
		line_style = '-'
		line_color = 'black'
	elif "x.rbt.int.rcu_htm" in filename:
		lab = "rcu-htm-rbt"
		marker = 'x'
		line_style = '--'
		line_color = 'black'
	else:
		lab = "ERROR LABEL"
		marker = ""
		line_color = "black"
		line_style = "-"

	return lab, marker, line_style, line_color

## We keep a dictionary where the keys are the filenames and values
## are arrays of arrays with the throughput values
d = dict()
nthreads_axis = []

for filename in sys.argv[1:]:
	fp = open(filename, "r")

	file_basename = '.'.join(os.path.basename(filename).split('.')[0:-2])

	if not file_basename in d:
		d[file_basename] = []

	throughput_axis = []
	nthreads_axis = []

	line = fp.readline()
	while line:
		tokens = line.split()
		if line.startswith("  num_threads:"):
			nthreads_axis.append(int(tokens[1]))
		elif line.startswith("Throughput(Ops/usec):"):
			throughput_axis.append(float(tokens[1]))
		elif line.startswith("  lookup_frac:"):
			lookups_pct = int(tokens[1])
		elif line.startswith("  insert_frac:"):
			inserts_pct = int(tokens[1])
			deletes_pct = 100 - lookups_pct - inserts_pct
		elif line.startswith("  max_key"):
			max_key = int(tokens[1])

		line = fp.readline()
	
	d[file_basename].append(throughput_axis)
	fp.close()


ax = plt.subplot("111")

## Now let's iterate over all filenames and plot the data
for f in d:
	array_of_avgs = get_average(d[f])
	lab, marker, line_style, line_color = get_label_from_filename(f)
	ax.plot(np.arange(len(nthreads_axis)), array_of_avgs, label=lab, \
	        marker=marker, linewidth=2, markersize=12, markeredgewidth=2, \
	        ls=line_style, color=line_color)

## Set the font
font = {'weight':'normal','size':14}
matplotlib.rc('font', **font)

## Mark the end of physical cores.
#plt.axvline(5, color='black', linestyle='--')

plt.title(prefix_large_number(max_key) + " keys - " + str(lookups_pct) + "% lookups")

ax.set_xlim(-0.4, 6.4)
ax.set_xlabel("Number of threads")
ax.set_ylabel("Throughput (Mops/sec)")

leg = ax.legend(ncol=1,loc="upper left",prop={'size':12})
leg.draw_frame(False)

ax.yaxis.grid(True)
plt.xticks(np.arange(len(nthreads_axis)), nthreads_axis)
plt.savefig("nthreads-VS-throughput.png", bbox_inches = 'tight')
