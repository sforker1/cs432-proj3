from pyspark import SparkContext

moviesFile = "/Users/sforker/PycharmProjects/cs432-proj3/MovieTweetings-master/latest/movies.dat"
sc = SparkContext("local", "movies app")
moviesData = sc.textFile(moviesFile).cache()
numAs = moviesData.filter(lambda s: 'a' in s).count()
numBs = moviesData.filter(lambda s: 'b' in s).count()
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))