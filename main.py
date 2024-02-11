import flask
from bson import json_util, ObjectId
import pymongo
import json
from bson.json_util import dumps
from bson.json_util import dumps
from bson import ObjectId
from flask import jsonify
app = flask.Flask(__name__)
uri = "mongodb+srv://user1:ali1234@cluster0.emakkdb.mongodb.net/?retryWrites=true&w=majority"
client1 = pymongo.MongoClient(uri)
client = pymongo.MongoClient("localhost", 27017)
db = client1.db1
collection = db.data
collection2 = db.PremierLeague
collectionv1 = db.PremierLeaguev1
collectionv3 = db.PremierLeaguev3
mostLikesCursor = collection.aggregate([
    {"$group": {"_id": "$userName", "sum_val": {"$sum": "$likes"}}},
    {"$sort": {"sum_val": -1}},
    {"$limit": 1}
])

# devices queries
devicesCursor = collection.aggregate([

    {"$group": {"_id": "$label", "value": {"$sum": 1}}},
    {"$project": {"_id": 1, "value": 1}},
    {"$sort": {"value": -1}},
    {"$limit": 4}
])
# insert data into an array
devicesList = list(devicesCursor)
# convert data into json string
devices = dumps(devicesList)
# convert data into json object
devicesJson = json.loads(devices)

# mostRetweeted queries
mostRetweetsCursor = collection.aggregate([
    {"$group": {"_id": "$userName", "sum_val": {"$sum": "$retweets"}}},
    {"$sort": {"sum_val": -1}},
    {"$limit": 1}
])

# mostReplys queries
mostReplyedCursor = collection.aggregate([
    {"$group": {"_id": "$userName", "sum_val": {"$sum": "$reply"}}},
    {"$sort": {"sum_val": -1}},
    {"$limit": 1}
])

# topInfluencers queries
topInfluencerCursor = collection.aggregate(
    [

        {"$project": {
            "userName": "$userName",
            'likes': '$likes',
            'retweets': '$retweets',
            'reply': '$reply',
            "image": "$profilePicture",
            'totalSum': {'$add': ['$likes', '$retweets', '$reply']},
        }
        },

        {"$project": {"_id": "$userName",
                      "likes": "$likes", 'retweets': '$retweets', "image": "$image",
                      'reply': '$reply', "score": {"$sum": "$totalSum"}}},

        {"$sort": {"score": -1}},
        {"$limit": 6}

    ]
)

# verified queries
verifiedCursor = collection.aggregate([

    {"$group": {"_id": "$verified", "value": {"$sum": 1}}},
    {"$project": {"_id": 1, "value": 1}}


])

# topUsers queries
topUsersCursor = collection.aggregate(
    [

        {
            "$group":
            {
                "_id": {"name":  "$userName", "email":  "$userLink",
                        "image": "$profilePicture", "link": "$user"},
                "tweets": {"$sum": 1}
            }

        },

        {"$sort": {"tweets": -1}},
        {"$limit": 5},


    ]
)

# age queries
createdDateCursor = collection.aggregate([

    {"$group": {"_id": "$accountCreatedYear", "value": {"$sum": 1}}},
    {"$sort": {"_id": 1}},
    {"$project": {"_id": 1, "accountCreatedYear": 1, "value": 1}}


])

# topHashtags queries
topHashtagCursor = collection.aggregate([
    {"$unwind": "$hashtags"},
    {"$group": {"_id": "$hashtags", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 60}
])
# topHashtags queries
topHashtagminiCursor = collection.aggregate([
    {"$unwind": "$hashtags"},
    {"$group": {"_id": "$hashtags", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 12}
])

# coHashtags queries
coHashtagCursor = collection.aggregate([

    {"$group": {"_id": "$hashtags", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
])

# top100 query
top100Cursor = collection.aggregate([
    {"$project": {
        "userName": "$userName",
        'likes': '$likes',
        'retweets': '$retweets',
        'image': "$profilePicture",
        'reply': '$reply',
        'email': '$email',
        'status': '$verified',
        "device": "$label",
        "date": "$dateString",
        'totalSum': {'$add': ['$likes', '$retweets', '$reply']},
    }
    },
    {"$sort": {"totalSum": -1}},
    {"$limit": 100}

]
)

# top10 query
top10Cursor = collection.aggregate([
    {"$project": {
        "userName": "$userName",
        'likes': '$likes',
        'retweets': '$retweets',
        'reply': '$reply',

        'totalSum': {'$add': ['$likes', '$retweets', '$reply']},
    }
    },
    {"$sort": {"totalSum": -1}},
    {"$limit": 6}

]
)

# coordinates
coordinatesCursor = collectionv3.aggregate([
    {"$project": {
        "name": "$location",
        "id": "$userName",
        "color": "$color",
        "city": "$location",
        "value": "$likes",
        'coordinates':  ['$longitude', '$latitude'],
    }
    },


]


)
coordinates2Cursor = collectionv3.aggregate([
    {"$project": {
        "name": "$location",
        "id": "$userName",
        "color": "$color",
        "city": "$location",
        "value": "$likes",
        'coordinates':  ['$latitude', '$longitude'],
    }
    },


]
)


# hours query
hoursCursor = collection.aggregate([

    {"$group": {"_id": "$hour", "value": {"$sum": 1}}},
    {"$sort": {"_id": 1}},




])
# lists (dump the data in the cursor inside a list)
verifiedList = list(verifiedCursor)
topHashtagList = list(topHashtagCursor)
topHashtagminiList = list(topHashtagminiCursor)
coHashtagList = list(coHashtagCursor)
createdDateList = list(createdDateCursor)
topUsersList = list(topUsersCursor)
topInfluencerList = list(topInfluencerCursor)
top100List = list(top100Cursor)
top10List = list(top10Cursor)
hoursList = list(hoursCursor)
coordinates2List = list(coordinates2Cursor)
coordinatesList = list(coordinatesCursor)

mostLikesList = list(mostLikesCursor)
# json (convert the list into a string json file)
verifiedUsers = dumps(verifiedList)
topHashtagsmini = dumps(topHashtagminiList)
topHashtags = dumps(topHashtagList)
coHashtags = dumps(coHashtagList)
topInfluencer = dumps(topInfluencerList)
createdDate = dumps(createdDateList)
topUsers = dumps(topUsersList)
mostLikes = dumps(mostLikesList)
top100 = dumps(top100List)
top10 = dumps(top10List)
hours = dumps(hoursList)
coordinates2 = dumps(coordinates2List)
coordinates = dumps(coordinatesList)

# jsonConvert (convert the json file into json file object)
verifiedUsersJson = json.loads(verifiedUsers)
topHashtagsJson = json.loads(topHashtags)
topHashtagsminiJson = json.loads(topHashtagsmini)
coHashtagsJson = json.loads(coHashtags)
createdDateJson = json.loads(createdDate)
topUsersJson = json.loads(topUsers)
topInfluencerJson = json.loads(topInfluencer)
mostLikesListJson = json.loads(mostLikes)
top100Json = json.loads(top100)
top10Json = json.loads(top10)
hoursJson = json.loads(hours)
coordinates2Json = json.loads(coordinates2)
coordinatesJson = json.loads(coordinates)
test = topInfluencerJson


@ app.route("/users")
def users():

    return {"verified": verifiedUsersJson, "created": createdDateJson, "topUsers": topUsersJson, "topInfluencers": topInfluencerJson}


@ app.route("/hashtag")
def hashtag():

    return {"hashtag": topHashtagsJson, "coHashtags": coHashtagsJson}


@ app.route("/dashboard")
def dashboard():

    return {"hashtag": topHashtagsJson, "devices": devicesJson}


@ app.route("/tweets")
def tweets():

    return {"hours": hoursJson, "top10": top10Json, "topHashtags": topHashtagsminiJson, "coHashtags": coHashtagsJson}


@ app.route("/top100")
def top100():

    return top100Json


@ app.route("/maps")
def maps():

    return coordinatesJson


@ app.route("/globe")
def globe():

    return coordinates2Json


@app.route("/add", methods=["POST"], strict_slashes=False)
def add_articles():

    title = request.json['title']
    body = request.json['body']

    article = Articles(
        title=title,
        body=body
    )

    db.session.add(article)
    db.session.commit()

    return article_schema.jsonify(article)


if __name__ == "__main__":
    app.run(debug=True, port=os.getenv("PORT", default=5000)))
