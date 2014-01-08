import os
import sys
from run import *
if __name__ == "__main__":
	pa = sys.argv[1]
	os.system('rm ../course/test')
	s = 'ln -s '+os.path.abspath('.')+"/"+pa + " "+os.path.abspath('../course/test')
	os.system(s)

	truehash = run(pa)
	os.chdir('../course')
	res =  os.popen('./run').read()
	ll = res.split('\n')
	myhash = []
	for l in ll:
		if l[:8] == "Checksum":
			myhash.append(l[10:])
		elif l[:14] == "Response time:":
			print "time: ", l[15:]

	print myhash == truehash
