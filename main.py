from distutils.debug import DEBUG
from pyspark import SparkContext
import itertools
import copy

def main():
    global DEBUG
    DEBUG = 0
    dataPrep()
    while(True):
        print("1. Find Movie Info")
        # print(".. Add a Movie Rating")
        print("2. Movie Recommendation")
        print("3. Movie Popularity Predictor")
        print("4. Enable/Disable Debug")
        print("5. Exit")
        userInput = input("Menu Option: ")

        if(userInput == "1"):
            findMovie()
        elif(userInput == "2"):
            movieRec()
        elif(userInput == "3"):
            moviePop()
        elif(userInput == "4"):
            if(DEBUG):
                print("\nDisabled\n")
                DEBUG = 0
            else:
                print("\nEnabled\n")
                DEBUG = 1
        elif(userInput == "5"):
            break
        else:
            print("\nIncorrect Option\n")

def dataPrep():
    moviesFile = "movies.dat"
    ratingsFile = "ratings.dat"
    global sc
    sc = SparkContext("local", "movies app")
    moviesData = sc.textFile(moviesFile).cache()
    moviesKey = moviesData.map(lambda x: (x.split("::")[0], (x.split("::")[1].split(" (")[0].lower(), x.split("::")[1].split("(")[1][:-1], x.split("::")[2].lower())))
    #moviesData.map(lambda x: (x.split("::")[1].split(" (")[0], x.split("::")[1].split("(")[1][:-1], x.split("::")[2]))
    #reduce list by average of same ratings
    
    #make title all uppercase!
    
    ratingsData = sc.textFile(ratingsFile).cache()
    ratingsKey = ratingsData.map(lambda x: (x.split("::")[1], (x.split("::")[2])))
    tempTuple = (0,0)
    tempRDD = ratingsKey.aggregateByKey(tempTuple, lambda a,b: (int(a[0]) + int(b), int(a[1]) + 1), lambda a,b: (int(a[0]) + int(b[0]), int(a[1]) + int(b[1])))
    finalResult = tempRDD.mapValues(lambda x: x[0] / x[1])
    # finalResult.foreach(lambda x: print(x))
    ratingsKey = finalResult.map(lambda x: (x[0], str(x[1])))
    # ratingsKey.foreach(lambda x: print(x))
    #ratingsKey = ratingsKey.groupByKey().mapValues(lambda x: sum(int(x[1])) / len(int(x[1])))
    #print("COUNT {}".format(ratingsKey.count()))
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
    # TODO
    # Filter by whole words?
    # Print if no genre

    userInput = input("\nTitle of Movie: ").lower()
    dataRDD = filterfinal
    
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

    sentence = sentence.lower()
    genre = genre.lower()

    dataRDD = filterfinal

    # Create weight for tracking relevance
    mappedData = dataRDD.map(lambda x: (x, 0))

    ageData = ageEst(mappedData, age)
    if(DEBUG): ageData.foreach(lambda x: print(x))

    ratingData = ratingEst(ageData, rating)
    if(DEBUG): ratingData.foreach(lambda x: print(x))

    genreData = genreEst(ratingData, genre)
    if(DEBUG): genreData.foreach(lambda x: print(x))

    sentenceData = sentenceEst(genreData, sentence)
    if(DEBUG): sentenceData.foreach(lambda x: print(x))

    orderedData = sentenceData.sortBy(lambda x: x[1])

    recAmt = input("How Many Recommendations Would You Like? ")

    recList = orderedData.take(int(recAmt))

    print()
    
    for x in recList:
        print("Title: " + x[0][0] + "\nYear: " + x[0][1] + "\nGenre(s): " + x[0][2] + "\nRating (1-10): " + x[0][3] + "\n")


def ageEst(data, age):
    newData = data.map(lambda x: (x[0], x[1] + (abs(int(float(x[0][1])) - int(float(age))) * 5)))
    return newData

def ratingEst(data, rating):
    newData = data.map(lambda x: (x[0], x[1] + (abs(int(float(x[0][3])) - int(float(rating))) * 10)))
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
    dataRDD = filterfinal

    title = input("Proposed Title: ")
    # year = input("Year of Release: ")
    genres = input("Genre(s): ")

    title = title.lower()
    genres = genres.lower()

    #check if commas
    titleList = title.split(" ")
    genreList = genres.split(" ")

    #order combination list by amount of elements, small -> large
    titleComb = []
    for x in range(1, len(titleList)+1):
        medium = [list(y) for y in itertools.combinations(titleList, x)]
        titleComb.extend(medium)
    if(DEBUG): print("Title Combo: {}".format(titleComb))
    
    holdingRDD = sc.emptyRDD()
    compareSize = 1
    ifFirstRun = 1
    tempTitleComb = copy.deepcopy(titleComb)
    
    # Check ordering
    while(tempTitleComb):
        comparison = tempTitleComb.pop(0)

        compareString = ""
        if(len(comparison) > 1):
            compareString = " ".join(comparison)
        else:
            compareString = comparison[0]
        
        if(DEBUG): print("compareString is {}".format(compareString))

        tempRDD = dataRDD.filter(lambda x: True if all(y in x[0] for y in compareString) else False)
        # tempRDD = dataRDD.filter(lambda x: str(compareString) in x)
        if(DEBUG): tempRDD.foreach(lambda x: print("Temp First: {}".format(x)))

        if(ifFirstRun):
            holdingRDD = holdingRDD.union(tempRDD)
            ifFirstRun = 0
        # What if unioned? for all similar len(comparisons)
        elif(tempRDD.count() > holdingRDD.count() and len(comparison) == compareSize):
            holdingRDD = tempRDD
        elif(tempRDD.count() > 0 and len(comparison) > compareSize):
            holdingRDD = tempRDD

        compareSize = len(comparison)
    
    if(DEBUG): holdingRDD.foreach(lambda x: print("First: {}".format(x)))


    holdingRDD1 = sc.emptyRDD()
    compareSize = 1
    ifFirstRun = 1
    tempTitleComb = copy.deepcopy(titleComb)

    # Check amount matching
    while(tempTitleComb):
        comparison = tempTitleComb.pop(0)

        tempRDD = dataRDD.filter(lambda x: True if all(y in x[0] for y in comparison) else False)
        if(DEBUG): tempRDD.foreach(lambda x: print("Temp Second: {}".format(x)))

        if(ifFirstRun):
            holdingRDD1 = holdingRDD1.union(tempRDD)
            ifFirstRun = 0
            if(DEBUG): print("First Run")
        # What if unioned? for all similar len(comparisons)
        elif(tempRDD.count() > holdingRDD1.count() and len(comparison) == compareSize):
            if(DEBUG): print("Second Op")
            holdingRDD1 = tempRDD
        elif(tempRDD.count() > 0 and len(comparison) > compareSize):
            if(DEBUG): print("Third Op")
            holdingRDD1 = tempRDD

        compareSize = len(comparison)

    if(DEBUG): holdingRDD1.foreach(lambda x: print("Second: {}".format(x)))
    
    # Check for all genres
    genreRDD = dataRDD.filter(lambda x: True if all(y in x[2] for y in genreList) else False)
    if(DEBUG): genreRDD.foreach(lambda x: print("Third: {}".format(x)))


    holdingRDD = holdingRDD.distinct()
    holdingRDD1 = holdingRDD1.distinct()
    genreRDD = genreRDD.distinct()

    finalRDD = holdingRDD
    if(finalRDD.intersection(holdingRDD1).count() > 0):
        finalRDD = finalRDD.intersection(holdingRDD1)
    
    if(finalRDD.intersection(genreRDD).count() > 0):
        finalRDD = finalRDD.intersection(genreRDD)
    
    calcRDD = finalRDD.map(lambda x: int(float(x[3])))

    predictedRating = calcRDD.mean()

    print("\nThe Predicted Rating Is: {0}".format(predictedRating))

    # Data to suggest that Movies involving this style are generally more reviewed and populat


main()
