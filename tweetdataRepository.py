# library untuk menghubungkan Python dan MongoDB
from pymongo import MongoClient
# Library untuk mengakses JSON
from bson import json_util, ObjectId
import json
# Library untuk memanipulasi data
from datetime import datetime
import pytz


class TweetDataRepository:

    def __init__(self):
        # # ----- db lokal
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.TwitterStream
        self.tweet_data = self.db.tweetsSentimentLocationAll
        # # # ----- db atlasku
        # self.client = MongoClient(
        #     "mongodb+srv://capstone:dteti@cluster0.fz15r5k.mongodb.net/?retryWrites=true&w=majority")
        # self.db = self.client.TwtStream
        # self.tweet_data = self.db.TwtSen
        # # ----- db atlas labib
        # self.client = MongoClient(
        #     "mongodb+srv://m001-student:m001-mongodb-basics@sandbox.n2kupvf.mongodb.net/?retryWrites=true&w=majority")
        # self.db = self.client.moniqq
        # self.tweet_data = self.db.qoeTweetSentimen
    # mengambil semua data tweet berdasarkan provider dan tanggal mulai dan tanggal berakhir

    def get_all_tweet_provider_lokasi_date_to_date(self, provider, lokasi, tanggal_awal, tanggal_akhir):
        from_str = tanggal_awal
        until_str = tanggal_akhir
        # until_str = "2022-06-18"

        from_date_str_gmt_7 = from_str + "T00:00:00+07:00"
        to_date_str_gmt_7 = until_str + "T23:59:59+07:00"
        # from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        # to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        from_date_gmt_7 = datetime.strptime(
            from_date_str_gmt_7, '%Y-%m-%dT%H:%M:%S%z')
        to_date_gmt_7 = datetime.strptime(
            to_date_str_gmt_7, '%Y-%m-%dT%H:%M:%S%z')
        from_date_gmt_0 = from_date_gmt_7.astimezone(
            pytz.timezone('Etc/Greenwich'))
        to_date_gmt_0 = to_date_gmt_7.astimezone(
            pytz.timezone('Etc/Greenwich'))
        # criteria = {"$and": [{"created": {"$gte": from_date, "$lte": to_date}}, {
        #     "provider": provider}]}
        aggregation = [
            {
                '$match': {
                    '$and': [
                        {
                            'provider': provider
                        }, {
                            'created': {
                                '$gte': from_date_gmt_0,
                                '$lte': to_date_gmt_0
                            }
                        }, {
                            'lokasi': lokasi
                        }
                    ]
                }
            },
            {
                '$project': {
                    'id': {
                        '$toString': '$_id'
                    },
                    'tweet_id': '$tweet_id',
                    'text': '$text',
                    'username': '$username',
                    'language': '$language',
                    'created': {
                        '$dateToString': {
                            'format': '%Y-%m-%dT%H:%M:%S:%L',
                            'date': '$created',
                            'timezone': '+07:00'
                        }
                    },
                    'created_date': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': '$created',
                            'timezone': '+07:00'
                        }
                    },
                    'created_time': {
                        '$dateToString': {
                            'format': '%H:%M:%S:%L',
                            'date': '$created',
                            'timezone': '+07:00'
                        }
                    },
                    'text_preprocessed': '$text_preprocessed',
                    'lokasi': '$lokasi',
                    'provider': '$provider',
                    'sentimen': '$sentimen'
                }
            }
        ]

        cursor = self.tweet_data.aggregate(aggregation)
        tweet_data = list(cursor)
        tweet_data = json.loads(json_util.dumps(tweet_data))
        return tweet_data
