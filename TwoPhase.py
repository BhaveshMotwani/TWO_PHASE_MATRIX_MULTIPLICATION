from pyspark import SparkContext
from collections import defaultdict
from operator import add
import sys

sc = SparkContext(appName="inf553")


def main():
    #Phase 1
	#Mapping For Phase 1
    a = sc.textFile(sys.argv[1])
    b = sc.textFile(sys.argv[2])
    aMat = a.map(lambda x: x.split(',')).map(lambda x: (x[1], ('A', x[0], x[2])))
    bMat = b.map(lambda x: x.split(',')).map(lambda x: (x[0], ('B', x[1], x[2])))
    Mata = aMat.collect()
    Matb = bMat.collect()

    	# Reducing For Phase1

def prone(x):
    r1 = list(x[0])
    r2 = list(x[1])
    r3 = defaultdict(int)
    for i in range(len(r1)):
        for j in range(len(r2)):
            r3[(int(r1[i][1]), int(r2[j][1]))] = int(r1[i][2]) * int(r2[j][2])
    return r3
    red1 = Mata.cogroup(Matb)
    final = red1.mapValues(prone).map(lambda x:x[1]).flatMap(lambda x: x.items())

    #Phase 2
    	
	#Mapping for Phase1
 
		#Mapping does not involve anything.The Rdd's are passed without any change.
    	
	# Reduction for Phase 2
   
    result = final.reduceByKey(lambda x,y:x+y).map(lambda x:str(x[0][0])+","+str(x[0][1])+"\t"+str(x[1])).coalesce(1).collect()
     
	#Writing Output
 
    outputfile = sys.argv[3]

    file = open(output,'w')
    for val in result:
	file.write("%s\n"%val)

if __name__ == '__main__':
    main()