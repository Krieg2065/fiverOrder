#!/usr/bin/env python
# encoding: utf-8
from flask import Flask,request,Response
import pymongo
import csv
import json
from bson import json_util

app = Flask(__name__)

try:
    mongo = pymongo.MongoClient(   
        "mongodb+srv://mongo12:MongoAdmin@cluster0.gypmqqj.mongodb.net/"
    )
    db = mongo.database
    mongo.server_info() #trigger exception if cannot connect to db
except:
    print("Error -connect to db")


#inserts all data in the database
@app.route('/data')
def data():
    header = ["id","title","type","description","release_year","age_certification","runtime","genres","production_countries","imdb_score"]
    csvfile = open('Netflix.csv', 'r')
    reader = csv.DictReader( csvfile )

    for each in reader:
        row={}
        for field in header:
            row[field]=each[field]
            
        print (row)
        db.netflix.insert_one(row)

    return "ok"




#1.	Insert the new movie and show. 
@app.route('/api', methods=['POST'])
def InsertMovie():
    if request.method=='POST':

        try:
            title = request.json["title"]
            type = request.json["type"]
            description = request.json["description"]
            release_year = request.json["release_year"]
            age_certification = request.json["age_certification"]
            runtime = request.json["runtime"]
            genres = request.json["genres"]
            production_countries = request.json["production_countries"]
            imdb_score = request.json["imdb_score"]
            cond = True
        except:
            cond = False
            pass


        if cond:
            id = db.netflix.insert_one(
                {
                'title': title,
                'type': type,
                'description': description,
                'release_year': release_year,
                "age_certification" : age_certification,
                "runtime": runtime,
                "genres": genres,
                "production_countries" : production_countries,
                "imdb_score"  : imdb_score
                }
            )
            resp = {
                "id" : str(id),
                'title': title,
                'type': type,
                'description': description,
                'release_year': release_year,
                "age_certification" : age_certification,
                "runtime": runtime,
                "genres": genres,
                "production_countries" : production_countries,
                "imdb_score"  : imdb_score
            }
            return resp
        else:
            return {'message': 'received'}

#2.	Update the movie and show information using title. (By update title, description and imdb score)

@app.route('/api/<string:fname>', methods=['PATCH'])
def UpdateMovie():
    try:
        title = request.json["title"]
        description = request.json["description"]
        imdb_score = request.json["imdb_score"]
        cond = True
    except:
        cond = False
        pass
    cur = db.netflix.find_one( { "title" : fname } )
    results = list(cur)
    # Checking the cursor is empty

    if cond and len(results) > 0:
        db.netflix.update_one(
            { "title" : fname },
            {"$set": {
                "title" : title,
                "description" : description,
                "imdb_score": imdb_score
            }},
            upsert=True
        )

        resp = json_util.dumps(cur)
        return Response(resp, mimetype = 'application/json')
    else:
        return {'message': 'received'}

#3.	Delete the movie and show information using title.

@app.route('/api/<string:fname>', methods=['DELETE'])
def DeleteMovie(fname):
    rec = db.netflix.find_one( { "title" : fname } )
    id = db.netflix.delete_one( { "title" : fname } )

    if id.deleted_count == 1:
        resp = json_util.dumps(rec)
        return Response(resp, mimetype = 'application/json')
    else:
        return "Movie not found"


#4.	Retrieve all the movies and shows in database.
@app.route('/api', methods=['GET'])
def GetMovies():
    rec = db.netflix.find( {} )
    resp = json_util.dumps(rec)
    return Response(resp, mimetype = 'application/json')



#5.	Display the movie and show’s detail using title.


@app.route('/api/<string:fname>', methods=['GET'])
def DetailsMovie(fname):

    cur = db.netflix.find_one( { "title" : fname } )
    results = list(cur)
    # Checking the cursor is empty
    if len(results)==0:
        return "Movie not found"
    else:
        resp = json_util.dumps(rec)
        return Response(resp, mimetype = 'application/json')


if __name__ == "__main__":
    app.run(debug=True)