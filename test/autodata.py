import sqlite3
import random
import os
from run import *

max_int = 100
max_char_size = 10


def gendata(path, table):
	f = open(path+'/schema', 'w')
	print >> f, len(table)
	conn = sqlite3.connect(path+'.db')
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
		ts.append((col_pk, col_int, col_char))
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
	f = open('Project.db', 'w')
	f.close()
	tables = [("project", 1, 12, 10, 10)]
	ts = gendata("Project", tables)
	conn = sqlite3.connect('Project.db')
	c = conn.cursor()	
	t = ts[0][0]+ts[0][1]+ts[0][2]
	f = open("Project"+'/query', 'w')
	print >> f, qnum
	for ik in range(qnum):
		li = range(len(t))
		random.shuffle(li)
		n = random.randint(1, len(t))
		tt = [t[i] for i in li[:n]]
		query = "SELECT "+" , ".join(tt)+" FROM " + tables[0][0] + ";"
		print >> f, query
		# print runquery(c, query)
	conn.commit()
	conn.close()

def gendata2(qnum):
	f= open('Join.db', 'w')
	f.close()
	tables = [("join1", 1, 10, 0, 10), ("join2", 1, 12, 1, 20)]
	ts = gendata("Join", tables)
	conn = sqlite3.connect('Join.db')
	c = conn.cursor()
	t = ts[0][0]+ts[0][1]+ts[0][2]+ts[1][0]+ts[1][1]+ts[1][2]
	f = open("Join"+"/query", 'w')
	print >> f, qnum
	for ik in range(qnum):
		li = range(len(t))
		random.shuffle(li)
		n = random.randint(1, len(t))
		tt = [t[i] for i in li[:n]]
		query = "SELECT "+" , ".join(tt)+" FROM " + tables[0][0] + " , "+tables[1][0]
		a = random.randint(0, len(ts[0][1])-1)
		b = random.randint(0, len(ts[1][1])-1)
		query += " WHERE "+ts[0][1][a]+" = "+ts[1][1][b] + " ; "
		print query
		print >> f, query
		print runquery(c, query)
	
	conn.commit()
	conn.close()

if __name__ == "__main__":
	# gendata1(10)
	gendata2(10)
