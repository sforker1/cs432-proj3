from pyspark import SparkContext
import itertools
# Web interface?


# moviesFile = "movies.dat" #"MovieTweetings-master/latest/movies.dat"
# sc = SparkContext("local", "movies app")
# moviesData = sc.textFile(moviesFile).cache()
# numAs = moviesData.filter(lambda s: 'a' in s).count()
# numBs = moviesData.filter(lambda s: 'b' in s).count()
# print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

def main():
    dataPrep()
    while(True):
        print("1. Find Movie Info")
        print(".. Add a Movie Rating")
        print("2. Movie Recommendation")
        print("3. Movie Popularity Predictor")
        userInput = input("Menu Option: ")

        if(userInput == "1"):
            findMovie()
        elif(userInput == "2"):
            movieRec()
        elif(userInput == "3"):
            moviePop()
        else:
            print("Incorrect Option")
            break

def dataPrep():
    moviesFile = "movies.dat"
    ratingsFile = "ratings.dat"
    sc = SparkContext("local", "movies app")
    moviesData = sc.textFile(moviesFile).cache()
    moviesKey = moviesData.map(lambda x: (x.split("::")[0], (x.split("::")[1].split(" (")[0], x.split("::")[1].split("(")[1][:-1], x.split("::")[2])))
    #moviesData.map(lambda x: (x.split("::")[1].split(" (")[0], x.split("::")[1].split("(")[1][:-1], x.split("::")[2]))
    #reduce list by average of same ratings
    
    #make title all uppercase!
    
    ratingsData = sc.textFile(ratingsFile).cache()
    ratingsKey = ratingsData.map(lambda x: (x.split("::")[1], (x.split("::")[2])))
    #combined = moviesKey.union(ratingsKey).reduceByKey(_ + _) #.reduceByKey(lambda x,y: x+y)
    # combine = moviesKey.join(ratingsKey).map(lambda x,y: x ++ y)
    # rdd3 = rdd2.keys()
    #rdd2.union()
    combine = moviesKey.union(ratingsKey).reduceByKey(lambda x,y : x+(y,))#moviesKey.cartesian(ratingsKey)
    remap = combine.map(lambda x: x[1])
    filter1 = remap.filter(lambda x: isinstance(x, tuple))
    global filterfinal 
    filterfinal = filter1.filter(lambda x: len(x) == 4)
    # return filterfinal
    #unioned = moviesKey.union(ratingsKey)
    #combine = unioned.combineByKey(to_list, append, extend)
    #filterfinal.foreach(lambda x: print(x))

def findMovie():
    userInput = input("\nTitle of Movie: ")
    dataRDD = filterfinal
    # names = dataRDD.map(lambda x: x[0])
    # names.foreach(lambda x: print(x))

    #uppercase
    #filter by whole words?
    #print if no genre

    movies = dataRDD.filter(lambda x: userInput in x[0])
    outputCount = movies.count()
    
    if(outputCount == 0):
        print("\nNo Movies Found with That Name!")
    elif(outputCount == 1):
        print("\nMovie Found!")
        movies.foreach(lambda x: print("Title: " + x[0] + "\nYear: " + x[1] + "\nGenre(s): " + x[2] + "\nRating (1-10): " + x[3]))
    else:
        print("\nMultiple Movies Found")
        movies.foreach(lambda x: print("Title: " + x[0] + "\nYear: " + x[1] + "\nGenre(s): " + x[2] + "\nRating (1-10): " + x[3] + "\n"))
    print()

    

def movieRec():
    age = input("Estimated Year: ")
    rating = input("Estimated Rating (1-10): ") 
    genre = input("Genre (Short|Action|Adventure|Comedy|Fantasy|Sci-Fi): ")
    sentence = input("Some Words to Describe Movie: ")

    dataRDD = filterfinal

    # Create weight for tracking relevance
    mappedData = dataRDD.map(lambda x: (x, 0))

    ageData = ageEst(mappedData, age)
    # ageData.foreach(lambda x: print(x))

    # print()

    ratingData = ratingEst(ageData, rating)
    # ratingData.foreach(lambda x: print(x))

    # print()

    genreData = genreEst(ratingData, genre)
    #genreData.foreach(lambda x: print(x))

    #print()

    sentenceData = sentenceEst(genreData, sentence)
    #sentenceData.foreach(lambda x: print(x))

    #print()

    orderedData = sentenceData.sortBy(lambda x: x[1])

    recAmt = input("How Many Recommendations Would You Like? ")

    recList = orderedData.take(int(recAmt))
    
    for x in recList:
        print(x)

    # .foreach(lambda x: print(x))


    # tempFunctionList = functionList

    # while(tempFunctionList):
    #     currentOrder = tempFunctionList.pop()

    #     ageOutcome = ageEst(age)
    #     # ratingOutcome = 

    #analysis -> order in decsending, prompt user for amt of recommendations


def ageEst(data, age):
    newData = data.map(lambda x: (x[0], x[1] + (abs(int(x[0][1]) - int(age)) * 5)))
    return newData

def ratingEst(data, rating):
    newData = data.map(lambda x: (x[0], x[1] + (abs(int(x[0][3]) - int(rating)) * 10)))
    return newData

def genreEst(data, genre):
    newData = data.map(lambda x: (x[0], x[1] - 25) if genre in x[0][2] else (x[0], x[1]))
    return newData

def sentenceEst(data, sentence):
    words = sentence.split()

    for y in words:
        data = data.map(lambda x: (x[0], x[1] - 15) if y in x[0][0] else (x[0], x[1]))
    return data


estimationFunc = [ageEst, ratingEst, genreEst, sentenceEst]
functionList = list(itertools.permutations(estimationFunc))
    
def moviePop():
    print("Third")

main()