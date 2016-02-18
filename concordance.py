import sys
import os
import itertools
from collections import OrderedDict
from collections import defaultdict
import multiprocessing
from multiprocessing import Pool
import logging
import timeit
from functools import partial
from cStringIO import StringIO
from itertools import islice


'''
algorithm focuses on idea that we do not know file size
provided algorithm which utilizes the single file input but uses islice to break into pieces
the file is read in chunks by the processes
no need to figure the line count by reading in chunks only issue is the offset to get the line numbers
the map processes builds a dictionary which is then partititoned and reduced
a multiprocess pool is employed to parse the file and again to do the reduce
islice also requires a custom peek function to figure out when to stop 

TODO: deal with punctuation and numbers, check performance
'''

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
CPU_COUNT = multiprocessing.cpu_count()

def timer(func):
	def smallfunctimer(*args,**kwargs):
		t=timeit.Timer(partial(func,*args,**kwargs))
		print "Function time: %s" % t.timeit(1)
	return smallfunctimer

def Map(c):
	return c()

def Reduce(item):
  return (item[0], (len(item[1]),sorted(item[1])))

def Partition(cList,processes_=1):
	totalDict = defaultdict(list)
	it = itertools.chain.from_iterable(x.data.items() for x in cList)
	for i in it:
		totalDict[i[0]].extend(i[1])
	return Pool(processes=processes_).map(Reduce, totalDict.items())

@timer
def runConcordance(fname, processes=1, chunk=2):
	Concordance.POOL_COUNT=processes
	Concordance.CHUNK=chunk
	clist=[Concordance(fname,x) for x in range(1,processes+1)]
	result=Partition(Pool(processes=processes).map(Map, clist),processes)
	c=Concordance('')		
	c.data = OrderedDict( sorted(result, key=lambda x:x[0]))
	print c

class Concordance(object):
	POOL_COUNT=0
	CHUNK=2

	def __init__(self,fname, id_=-1):
		self.fname=fname
		self.data={}
		self.id_=id_
		self.start=-1
	
	def readslice(self, f ):
		if self.start<0:
			self.start=(self.id_)*self.CHUNK-self.CHUNK
		else:
			self.start=((self.POOL_COUNT-1)*self.CHUNK)
		end=self.start+self.CHUNK
		f=islice(f,self.start,end)
		return f		

	def process(self, line, linenum, mydict):
		line=line.strip().lower().split()
		for word in line:
			mydict[word.strip().lower()].append(linenum)
		return (linenum+1)
		
	def peek(self,lines,linenum, mydict):
		line=next(lines, None)		
		if line:
			return self.process(line, linenum, mydict)		
		return False

	def run(self):
		slicecount=0
		linenum=1
		mydict = defaultdict(lambda: [])	
		with open(self.fname) as f:		
			while(linenum):	
				if self.id_>0:
					lines=self.readslice(f)
					linenum=(self.id_)*self.CHUNK-self.CHUNK+1+(slicecount*self.CHUNK*self.POOL_COUNT)
				linenum=self.peek(lines, linenum, mydict)
				for line in lines:
					linenum=self.process(line,linenum,mydict)
				slicecount+=1
		self.transform(mydict)
		return self
	
	def transform(self, myDict):
		self.data=OrderedDict( sorted(myDict.items(), key=lambda x:x[0]))	
		#return OrderedDict( sorted(myDict.items(), key=lambda x:x[0]))


	def __call__(self):
		return self.run()
		
	def __str__(self):
		io=StringIO()
		if self.id_==-1:
			io.write('Concordance Output\n')
		else:
			io.write('Pool-id:%d\n' % self.id_)
		io.write('%-15s %5s  %s\n' %('Word', 'Count', 'LineNum'))
		for key, value in self.data.items():
			io.write('%-15s: {%5d: %s}\n' % (key, value[0], ','.join(map(str,value[1]))))
		contents = io.getvalue()
		io.close()
		return contents
	
if __name__=='__main__':
	fname='test.txt'
	chunks=20
	processes=3
	fname=raw_input('please enter the file you want to parse: ')
	runConcordance(fname, 3,3)

	
