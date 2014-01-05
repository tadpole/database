import os
import sqlite3
def hash(s):
	return os.popen('./hash "'+s+'"').read()[:-1]

def addhash(a, b):
	return hex((int(a,16)+int(b,16)) % (1 << 32)).upper()[2:-1]

def runquery(c, query):
	lhash = []
	for row in c.execute(query):
		lhash.append(hash(','.join(repr(item.encode('utf8') if isinstance(item, unicode) else item) for item in row)))
	return reduce(addhash, lhash)

def run(path):
	f = open(path+"/query", 'r')
	l = f.readlines()
	conn = sqlite3.connect('example.db')
	c = conn.cursor()
	truepath = []
	for q in l[1:]:
		q = q[:-1]
		truepath.append(runquery(c, q))
	conn.close()
	f.close()
	return truepath

if __name__ == "__main__":
	print run('Project')
