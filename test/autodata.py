import sqlite3
import random
import os
from run import *

max_int = 100
max_char_size = 10


def gendata(path, table):
	f = open(path+'/schema', 'w')
	print >> f, len(table)
	conn = sqlite3.connect('example.db')
	c = conn.cursor()
	ct = ""
	ts = []
	for tablename, n_pk, n_int, n_char, linenum in table:
		print >> f, tablename
		n = n_pk+n_int+n_char
		col_pk = [tablename+"_id"+str(i) for i in range(n_pk)]
		col_int = [tablename+"_int"+str(i) for i in range(n_int)]
		col_char = [tablename+"_char"+str(i) for i in range(n_char)]
		ct = "CREATE TABLE "+tablename + " ("
		ctl = []
		print >> f,  n
		for i in col_pk:
			print >> f, i+"\tINTEGER"
			ctl.append(i+" INTEGER")
		for i in col_int:
			print >> f, i+"\tINTEHER"
			ctl.append(i+" INTEGER")
		for i in col_char:
			print >> f, i+"\tVARCHAR(100)"
			ctl.append(i+" TEXT")
		ts.append(col_pk+col_int+col_char)
		ct += ",".join(ctl)
		ct += ")"
		c.execute(ct)
		print >> f, n_pk
		for i in col_pk:
			print >> f, i
	f.close()
	f = open(path+'/statistic', 'w')
	print >> f, 0
	f.close()
	value = ""
	for tablename, n_pk, n_int, n_char, linenum in table:
		f = open(path+"/"+tablename+".data", 'w')
		print >> f, linenum
		for i in range(linenum):
			l = [str(i)]
			for i in range(n_pk-1):
				num = random.randint(1, linenum)
				l.append(str(num))
			for i in range(n_int):
				num = random.randint(1, max_int)
				l.append(str(num))
			for i in range(n_char):
				n = random.randint(1,max_char_size)
				s = "'"
				for j in range(n):
					s += random.choice('abcdefghijklmnoprstuvwxyz')
				s += "'"
				l.append(s)
			value = ",".join(l)
			c.execute("INSERT INTO "+tablename+" VALUES ("+value+")")
			print >> f, value
		f.close()
	conn.commit()
	c.close()
	return ts

def gendata1(qnum):
	f = open('example.db', 'w')
	f.close()
	tables = [("project", 1, 12, 10, 10)]
	ts = gendata("Project", tables)
	conn = sqlite3.connect('example.db')
	c = conn.cursor()	
	t = ts[0]	
	f = open("Project"+'/query', 'w')
	print >> f, qnum
	for ik in range(qnum):
		li = range(len(t))
		random.shuffle(li)
		n = random.randint(1, len(t))
		tt = [t[i] for i in li[:n]]
		query=  "SELECT "+" , ".join(tt)+" FROM " + tables[0][0] + ";"
		print >> f, query
		print runquery(c, query)
	conn.commit()
	conn.close()


if __name__ == "__main__":
	gendata1(10)
