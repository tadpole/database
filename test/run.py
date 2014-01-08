import os
import sqlite3
import sys

def hash(s):
	return os.popen('./hash "'+s+'"').read()[:-1]

def addhash(a, b):
	return hex((int(a,16)+int(b,16)) % (1 << 32)).upper()[2:-1]

def runquery(c, query):
	lhash = []
	res = []
	for row in c.execute(query):
		s = ','.join(repr(item.encode('utf8') if isinstance(item, unicode) else item) for item in row)
		res.append(s)
		lhash.append(hash(s))
	if lhash == []:
		return ("0", [])
	return (reduce(addhash, lhash),res)

def run(path):
	logname = path+".sol"
	if os.path.isfile(logname):
		f1 = open(logname, 'r')
		res = eval(f1.readlines()[0])
		f1.close()
		return res
	f1 = open(logname, 'w')
	f = open(path+"/query", 'r')
	l = f.readlines()
	conn = sqlite3.connect(path+'.db')
	c = conn.cursor()
	truepath = []
	for q in l[1:]:
		q = q[:-1]
		res = runquery(c, q)
		if q[:6] != "INSERT":
			truepath.append(res[0])
		# print res[1]
	conn.close()
	f.close()
	print >> f1, truepath
	f1.close()
	return truepath

if __name__ == "__main__":
	path = sys.argv[1]
	print run(path)
