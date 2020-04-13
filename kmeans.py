from pyspark import SparkContext, SparkConf
import math

def haversine(x1,x2):
	'''
		x1: first point with coordinates
		x2: second point with coordinates
	'''

	f1 = x1[0]
	f2 = x2[0]
	l1 = x1[1]
	l2 = x2[1]
	df = f1 - f2
	dl = l1 - l2
	R = 6371000

	a = math.sin(df/2)**2 + math.cos(f1)*math.cos(f2)*math.sin(dl/2)**2
	c = 2*math.atan2(math.sqrt(a),math.sqrt(1-a))
	d = R * c
	
	return d

def get_labels(points,centers):
    '''
		points: points[0] x coordinate, points[1] y coordinate
		centers: list of points
	''' 

	#init minimun distance to infinity
        mindst = float("+inf")
        temp = 0
	center = 0
        for i in range(len(centers)):
		temp = haversine(points,centers[i])
                #temp = ((points[0]-centers[i][0])**2 + (points[1]-centers[i][1])**2)
                if(temp<mindst):
                        center = i
			mindst = temp
	
	#return the index of closest center for this point
        return center

conf = SparkConf().setAppName('kMeans')
sc = SparkContext(conf=conf)

# read dataset,and split it in 100 files
data = sc.textFile("hdfs://master:9000/yellow_tripdata_1m.csv",100)
#split it by ','
data = data.map(lambda line: line.split(','))
#keep only x and y of startpoint
data = data.map(lambda line: (float(line[3]),float(line[4])))
#Delete points with 0 coordinates 
data = data.filter(lambda line: line[0]!=0 and line[1]!=0)


iterations = 0
max_iter = 3

# first centroid are the first five elements of the dataset
centroids = data.take(5)

while iterations < max_iter:
        points_and_labels = data.map(lambda p: (get_labels(p,centroids),(p,1)))
        points_and_labels = points_and_labels.reduceByKey(lambda a,b: ((a[0][0]+b[0][0],a[0][1]+b[0][1]),a[1]+b[1]))
        centroids = points_and_labels.mapValues(lambda v: (v[0][0]/v[1],v[0][1]/v[1])).collectAsMap()
	iterations = iterations + 1

print "\nCentroid Coordinates"
for i in range(len(centroids)):
        print "["+str(centroids[i][0])+","+str(centroids[i][1])+"]"
