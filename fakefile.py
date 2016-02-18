wordsonline='our car is running fast\n'
filename='testfile.txt'
with open(filename,'w+') as f:
	count=0
	while(count<10000000000):
		f.write(wordsonline)
		count=count+1


