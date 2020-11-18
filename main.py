import pymongo
from pymongo import MongoClient

def getArtistsWithHighestAverageScore(db):
    # Use the aggregate function and group by artist name
    results = db.reviews.aggregate([
        {
            "$group": {
                "_id": "$artist",
                "avgScore": {"$avg": "$score"},
                "numReviews": {"$sum": 1}
            }
        }
    ])

    # In order to operate on results, move them from cursor to list
    resultsList = []
    for result in results:
        resultsList.append(result)

    # Sort list in descending order by average score
    resultsList.sort(key=getAvgScore, reverse=True)
    print("\nTop-rated Artists by Average Score:")
    print("-----------------------------------")
    for i in range(10):
        result = resultsList[i]
        print(str(i + 1) + ") " + result['_id'] + " :: Average Score: " +
                str(round(result['avgScore'], 1)) + ", Number of Reviews: " + 
                str(result['numReviews']))

    # Remove artists with only 1 or 2 reviews, then show new results
    newResults = removeOutlierAvgs(resultsList)
    print("\nTop-rated Artists by Average Score (3 or more reviews):")
    print("-------------------------------------------------------")
    for i in range(10):
        result = newResults[i]
        print(str(i + 1) + ") " + result['_id'] + " :: Average Score: " +
                str(round(result['avgScore'], 1)) + ", Number of Reviews: " + 
                str(result['numReviews']))

def getAvgScore(json):
    try:
        return json['avgScore']
    except KeyError:
        return 0.0

def removeOutlierAvgs(jsonList):
    newList = []
    for item in jsonList:
        if item['numReviews'] >= 3:
            newList.append(item)

    return newList

def getPreferredGenresByYear(db):
    # Perform an aggregate operation to "join" collections
    results = db.reviews.aggregate([
        # First we lookup the genre of each review
        {
            "$lookup": {
                "from": "genres",
                "localField": "_id",
                "foreignField": "_id",
                "as": "albumGenre"
            }
        },
        # Then we merge the genre into the actual review object
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [
                        { "$arrayElemAt": [ "$albumGenre", 0 ] }, "$$ROOT"
                    ]
                }
            }
        },
        {
            "$project": {"albumGenre": 0}
        },
        # We then get an average score and count for each genre/year pairing
        {
            "$group": {
                "_id": {"genre":"$genre", "year":"$pub_year"},
                "avgScore": {"$avg": "$score"},
                "numReviews": {"$sum": 1}
            }
        }
    ])

    # Move objects from cursor to list
    resultsList = []
    for result in results:
        resultsList.append(result)

    # Get list of genres
    genres = getGenres(db)

    # Prompt user to select a genre they want to see stats for
    print("\nPlease select a genre to see scores for:")
    print("----------------------------------------")
    for i in range(len(genres)):
        genre = "undefined" if genres[i] == "" else genres[i]
        print(str(i + 1) + ") " + genre)

    choice = getInputFromUser()
    chosenGenre = genres[choice - 1]

    # Extract all objects of the chosen genre
    scoresList = extractScoresByGenre(chosenGenre, resultsList)

    # Present scores to user
    print("\nAverage scores by review year:")
    print("------------------------------")
    for result in scoresList:
        print("Year: " + result['_id']['year'] + ", Average Score: " +
                str(round(result['avgScore'], 1)) + ", Number of Reviews: " +
                str(result['numReviews']))

def getGenres(db):
    # Get list of genres from db
    results = db.genres.distinct("genre")

    return results

def extractScoresByGenre(genre, json):
    results = []
    for item in json:
        if item['_id']['genre'] == genre:
            results.append(item)

    results.sort(key=getYear)

    return results

def getYear(json):
    try:
        return json['_id']['year']
    except KeyError:
        return "1000"

def getAverageScoreByGenre(db):
    # Perform an aggregate operation to "join" collections
    results = db.reviews.aggregate([
        # First we lookup the genre of each review
        {
            "$lookup": {
                "from": "genres",
                "localField": "_id",
                "foreignField": "_id",
                "as": "albumGenre"
            }
        },
        # Then we merge the genre into the actual review object
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [
                        { "$arrayElemAt": [ "$albumGenre", 0 ] }, "$$ROOT"
                    ]
                }
            }
        },
        {
            "$project": {"albumGenre": 0}
        },
        # We then get an average score and count for each genre
        {
            "$group": {
                "_id": "$genre",
                "avgScore": {"$avg": "$score"},
                "numReviews": {"$sum": 1}
            }
        }
    ])

    # Move results from cursor to list
    resultsList = []
    for result in results:
        resultsList.append(result)

    resultsList.sort(key=getAvgScore, reverse=True)

    # Present user with scores
    print("\nAverage scores by genre:")
    print("------------------------")
    for result in resultsList:
        genre = "undefined" if result['_id'] == "" else result['_id']
        print("Genre: " + genre + ", Average Score: " + str(round(result['avgScore'], 1)) +
                ", Number of Reviews: " + str(result['numReviews']))

def getDistrobutionOfScores(db):
    # Get count of each score, as well as percentage
    nums = db.reviews.estimated_document_count();

    results = db.reviews.aggregate([
        {
            "$group": {
                "_id": "$score",
                "count": { "$sum": 1}
            }
        },
        {
            "$project": { 
                "count": 1, 
                "percentage": { 
                    "$multiply": [
                        { "$divide": [
                            "$count", {"$literal": nums }
                        ]}, 100
                    ]
                }
            }
        }
    ])

    # Move results from cursor to list
    resultsList = []
    for result in results:
        resultsList.append(result)

    # Combine stats in range groups
    groups = groupByScores(resultsList)

    # Present user with score statistics
    print("\nDistribution of scores among all reviews:")
    print("-----------------------------------------")
    for group in groups:
        groupCount = str(groups[group]['count'])
        groupPercent = str(round(groups[group]['percentage'], 3))
        print("Score Range: " + group + ", Number Given: " + groupCount +
                ", Percentage: " + groupPercent + "%")

def groupByScores(json):
    groups = {
            "0.0 - 0.9": {"count": 0, "percentage": 0.0},
            "1.0 - 1.9": {"count": 0, "percentage": 0.0},
            "2.0 - 2.9": {"count": 0, "percentage": 0.0},
            "3.0 - 3.9": {"count": 0, "percentage": 0.0},
            "4.0 - 4.9": {"count": 0, "percentage": 0.0},
            "5.0 - 5.9": {"count": 0, "percentage": 0.0},
            "6.0 - 6.9": {"count": 0, "percentage": 0.0},
            "7.0 - 7.9": {"count": 0, "percentage": 0.0},
            "8.0 - 8.9": {"count": 0, "percentage": 0.0},
            "9.0 - 10.0": {"count": 0, "percentage": 0.0}
    }

    for item in json:
        score = item['_id']
        count = item['count']
        percentage = item['percentage']
        group = ""
        if score <= 0.9:
            group = "0.0 - 0.9"
        elif score <= 1.9:
            group = "1.0 - 1.9"
        elif score <= 2.9:
            group = "2.0 - 2.9"
        elif score <= 3.9:
            group = "3.0 - 3.9"
        elif score <= 4.9:
            group = "4.0 - 4.9"
        elif score <= 5.9:
            group = "5.0 - 5.9"
        elif score <= 6.9:
            group = "6.0 - 6.9"
        elif score <= 7.9:
            group = "7.0 - 7.9"
        elif score <= 8.9:
            group = "8.0 - 8.9"
        else:
            group = "9.0 - 10.0"

        # Update stats in specified group
        oldCount = groups[group]['count']
        oldPercent = groups[group]['percentage']
        newCount = oldCount + count
        newPercent = oldPercent + percentage
        groups[group]['count'] = newCount
        groups[group]['percentage'] = newPercent

    return groups

def searchReviewsByTerm(db):
    # Get user to input a term to search for
    term = input("\nPlease enter a term to search for.\n>> ")

    # Find all review contents which contain that term
    results = db.content.find({"content": {"$regex": "" + term}})

    # Move results from cursor to list
    resultsList = []
    for result in results:
        resultsList.append(result)

    # Prompt user to select a random review to read
    print("\n" + str(len(resultsList)) + " reviews were found that use the term '" +
            term + "'")

    # If no reviews were found, then return
    if len(resultsList) is 0:
        return

    print("Please enter a number between 0 and " + str(len(resultsList)) + " to read that review")
    choice = getInputFromUser()

    while choice not in range(len(resultsList)):
        print("Please enter a number in the range 0 to " + str(len(resultsList)))
        choice = getInputFromUser()

    review = resultsList[choice]
    # Retrieve extra info on this review
    extraResults = db.reviews.find({"_id": review["_id"]})
    reviewInfo = extraResults[0]

    artist = reviewInfo['artist']
    album = reviewInfo['title']
    score = str(reviewInfo['score'])
    content = formatContent(review['content'])

    print("\nArtist: " + artist + " | Album: " + album + " | Score: " + score + "\n")
    print(content)
    input("\nPress Enter to Continue...")

def formatContent(content):
    return content.replace("\\n", "\n").replace("\\", "").replace("    ", "")

def presentQueryMenu():
    print()
    print("Please choose from the queries below:")
    print("-------------------------------------")
    print("1) Get artists with highest average score")
    print("2) Get preferred genres by year")
    print("3) Get average score by genre")
    print("4) Get distribution of scores")
    print("5) Search for terms in reviews")
    print("0) Exit")

def getInputFromUser():
    print()
    choiceStr = input(">> ")

    return int(choiceStr)

def dispatchChoice(choice, db):
    if choice is 0:
        return True
    elif choice is 1:
        getArtistsWithHighestAverageScore(db)
    elif choice is 2:
        getPreferredGenresByYear(db)
    elif choice is 3:
        getAverageScoreByGenre(db)
    elif choice is 4:
        getDistrobutionOfScores(db)
    elif choice is 5:
        searchReviewsByTerm(db)
    else:
        print("\nThat is not a valid choice.")

    return False

def main():
    cluster = MongoClient("mongodb+srv://benjamin:Nynergy17@cluster0.izkwv.mongodb.net/pitchfork?retryWrites=true&w=majority")
    db = cluster['pitchfork']

    quit = False
    while not quit:
        presentQueryMenu()
        choice = getInputFromUser()
        quit = dispatchChoice(choice, db)

    print("Now exiting...")

if __name__ == "__main__":
    main()
