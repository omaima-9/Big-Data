
import pymongo
from pymongo import MongoClient
import urllib.parse
from bson.json_util import dumps

username = urllib.parse.quote_plus('<<USERNAME>>') # replace <<USERNAME>> with your own MongoDB username
password = urllib.parse.quote_plus("<<PASSWORD>>") # replace <<PASSWORD>> with your own MongoDB password

url = "mongodb+srv://{}:{}@<<CLUSTERNAME>>.qgup7.mongodb.net/<<DATABASENAME>>?retryWrites=true&w=majority".format(username, password) # replace <<CLUSTERNAME>> with your cluster's name and <<DATABASENAME>> with your database's name
cluster = MongoClient(url)

db = cluster["<<DATABASENAME>>"] # replace <<DATABASENAME>> with your database's name
collection = db["watch"]

# -✔️- Reading movies from CSV into documents

import pandas as pd

df = pd.read_csv('C:/Users/LENOVO/Downloads/moviestv.csv') # edit path accordingly to read from where the moviestv.csv file is 
df = df.rename(columns={"Year of Release": "Year", "IMDB Rating": "Rating"})

i,j = df.shape

movies_tvshows = []

for x in range(0,i):
    movies_tvshows = movies_tvshows + [{'Type': '%s' % (df.loc[x].Type), 'Title': '%s' % (df.loc[x].Title), 'Year of Release': '%d' % (df.loc[x].Year), 'Director': '%s' % (df.loc[x].Director), 'Genre': '%s' % (df.loc[x].Genre), 'IMDB Rating': '%d' % (df.loc[x].Rating)} ]

# -✔️- Inserting the movies and shows into our collection

collection.insert_many(movies_tvshows)

# -✔️- Counting how many movies and tv shows available

movie_count = collection.count_documents({"Type":"Movie"})
tvshow_count = collection.count_documents({"Type":"TV Show"})

print("There are",movie_count,"movies and",tvshow_count,"tv shows")

# -✔️- Update an entry

beforeCH = collection.find({"Title":"Miracle In Cell No. 7"})
print(dumps(beforeCH,indent=3))

collection.update_one({"Title":"Miracle In Cell No. 7"},{"$set":{"Language":"Turkish"}})

afterCH = collection.find({"Title":"Miracle In Cell No. 7"})
print(dumps(afterCH,indent=3))

# -✔️- Insert a new tv show 

new_show = {
    "Year of Release": 2020,
    "Director": "Jason Sudeikis",
    "Type": "TV Show",
    "Title": "Ted Lasso",
    "Genre": "Drama",
    "IMDB Rating": 8.9
}

collection.insert_one(new_show)

# -✔️- Adding a duration attribute to all movies, with default value 90 minutes

collection.update_many({"Type":"Movie"},{"$set":{"Duration":"90 Minutes"}})

# -✔️- Deleting an show or movie released in 2017

collection.delete_many({"Year of Release":2017})

# -✔️- Finding action movies 

action_movies = collection.find({"Type":"Movie","Genre":"Action"})
print(dumps(action_movies,indent=3))

# -✔️- Getting the top rated movie and tv show
best_movie = collection.find({"Type":"Movie"}).sort("IMDB Rating",-1).limit(1)
best_show = collection.find({"Type":"TV Show"}).sort("IMDB Rating",-1).limit(1)

for x in best_movie:
    print("Best movie is",x["Title"],"from",x["Year of Release"],"with a rating of",x["IMDB Rating"])

for x in best_show:
    print("Best show is",x["Title"],"from",x["Year of Release"],"with a rating of",x["IMDB Rating"])


