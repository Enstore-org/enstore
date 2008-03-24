#!/usr/bin/env python

# enprof.py -- a tool to see the import dependence of a moudle
#
# enprof first searches every *.py file in ${PYTHONPATH}:${PYTONLIB}:.
# and builds an import list for each of them. To calculate the import
# dependency of a certain modules, it recursively search the list until
# no new modules can be reached.

import os
import string
import sys

xref = {}	# cross reference list for each known module

# scanning the file for import

def scan(path, file):	# scanning the file
	imports = []
	f = open(os.path.join(path, file))
	for line in f.readlines():
		# handle comments
		word = string.split(string.split(line, "#")[0])
		l = len(word)

		if l:
			# from ... import ...
			if word[0] == "from" and l > 3:
				if word[2] == "import":
					if not word[1] in imports:
						imports.append(word[1])
			elif word[0] == "import" and l > 1:
				for j in range(1, l):
					imps = string.split(word[j], ",")
					for k in imps:
						if k and not k in imports:
							imports.append(k)

	f.close()
	return file[:-3], imports

# trace(m, imports) -- trace an import chain on m

def trace(m, imports, traced):
	#print "M",m
	if not xref.has_key(m):
		return
	if m in traced:
		return
	for i in xref[m]['imports']:
		traced.append(m)
		trace(i, imports, traced)
		if not i in imports:
			imports.append(i)
	#print "TRACED",traced

# show(m, result) -- show the result

def show(m, result):
	result.sort()
	print m+": ("+os.path.join(xref[m]['path'], m)+".py)"
	for j in result:
		if j in xref.keys():
			print '\t'+j+" ("+os.path.join(xref[j]['path'], j)+".py)"
		else:
			print '\t'+j

# usage() -- show the usage

def usage():
	print "usage: %s [module ...]"%(sys.argv[0])
	print
	print '       where module is the name of a module without ".py" suffix'
	print '       if there is no module listed, all known modules will be listed.'

# get the python path for all library locations

if len(sys.argv) > 1 and sys.argv[1] == "-h":
	usage()
	sys.exit(0)

if os.environ['PYTHONPATH']:
	pythonpath = string.split(os.environ['PYTHONPATH'], ":")
else:
	pythonpath = []

if os.environ['PYTHONLIB']:
	pythonpath.append(os.environ['PYTHONLIB'])

pythonpath.append(".")	# current directory

# scanning the module areas

for path in pythonpath:
	for file in os.listdir(path):
		if file[-3:] == ".py":
			f, d = scan(path, file)
			xref[f] = {'path':path, 'imports':d}

if len(sys.argv) > 1:
	modules = sys.argv[1:]
else:
	modules = xref.keys()

for i in modules:
	result = []
	traced = []
	if xref.has_key(i):
		trace(i, result, traced)
		show(i, result)
	else:
		print "Don't know about "+i
