from pymongo import MongoClient
import pprint, sys
import matplotlib.pyplot as plt
from itertools import groupby
import tldextract
import numpy as np
import math, collections

def slice_odict(odict, start=None, end=None):
    return collections.OrderedDict([
        (k,v) for (k,v) in odict.items() 
        if k in list(odict.keys())[start:end]
    ])

# @info: return variance of ttls
def check_variance(g_dict):
	ttls = []
	variances = []
	for _, vals in g_dict.items():
		for v in vals:
			if v['ttl'] is not None:
				ttls.append(v['ttl'])

		# calculate variance per unique domain (each line on graph)
		variances.append(np.var(np.array(ttls)))
		ttls.clear()
	return max(variances) 
			
def graph_it(r_name, g_dict):
	# color pallete for graph
	pallete = plt.get_cmap('Set1')
	num = 0

	marks = np.array([20180401 , 20180415 , 20180501 , 20180515 , 20180601 , 20180615 , 20180701 , 20180715 ])
	#max g_dict size = 8
	for dname, vals in g_dict.items():
		num+=1
		y_vals = np.full(8, np.nan)
		for v in vals: #[3]
			for i, m in enumerate(marks):
				if m == v['f_date']:
					y_vals[i] = v['ttl']

		ymask = np.isfinite(y_vals)
		plt.plot(marks[ymask], y_vals[ymask], color=pallete(num), label=dname, marker='o')

		plt.legend(loc='best')
		plt.title("Domains pointing to response: " + r_name)
		plt.xlabel("Date Observed")
		plt.ylabel("Time-To-Live Value")

		# style
		plt.style.use('seaborn-darkgrid')
	plt.show()

# @info: make graphs for everything in list arr
# @use: visualize data
# TODO: numbers on top of data, strech graph
def make_graphs(r_name, g_dict):
	to_del = [] 
	for k, v in g_dict.items():
		for val in v:
			if val['ttl'] is None or 'None':
				v.remove(val)
		if len(v) <= 1:
			to_del.append(k)
			continue

	#delete entries with <= 1 dates
	for key in to_del:
		del g_dict[key]

	#check if dict is not empty
	#check variance to be > 30000 (arbituary - gives ttl diff. of 1000+)
	#chunk g_dict to graph function to reduce clutter
	if g_dict:
		variance = check_variance(g_dict) 
		if  variance > 10000:
			print("var of", r_name, variance)
			if len(g_dict) > 8:
				ordered_dict = collections.OrderedDict(g_dict)
				for i in range(math.ceil(len(g_dict)/8)):
					graph_it(r_name, slice_odict(ordered_dict, start=i*8, end=i*8+8))
			else:
				graph_it(r_name, g_dict)
				
	return

def main():
	# @info: set up mongodb connection
	client = MongoClient("212.201.49.38")
	db = client['ttl']
	collection = db['A_data']

	# @info: query DB to return all results
	call = collection.find({'response': {'$regex': 'cdn'}})
	print(call.count(), "records fetched")
	#pprint.pprint(call[1])
	
	g_count = 0
	for x in range(call.count()):
		# needed for sorting
		data = list(call[x]['dates'])
		data.sort(key=lambda a: a['q_name'][4:])
		
		#titlepage(call[x]['response'])
		g_count +=1
		g_dict = {}

		#group each unique q_name under each response
		for key, group in groupby(data, lambda a: a['q_name']):
			# print(call[x]['response'], "contains", key)
			g_dict[key] = list(group)

		make_graphs(call[x]['response'], g_dict)
		#print(g_count, "-----------------------------")


if __name__ == "__main__":
    main()