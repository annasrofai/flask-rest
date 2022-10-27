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
        self.tweet_data = self.db.tweetsSentiment5
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

    def get_all_tweet_saja_provider_date_to_date(self, provider, tanggal_awal, tanggal_akhir):
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
            {"$match":
                {"$and": [{"created": {"$gte": from_date_gmt_0, "$lte": to_date_gmt_0}}, {
                    "provider": provider}]}
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
                    'provider': '$provider',
                    'sentimen': '$sentimen'
                }
            }
        ]

        cursor = self.tweet_data.aggregate(aggregation)
        tweet_data = list(cursor)
        tweet_data = json.loads(json_util.dumps(tweet_data))
        return tweet_data

    #  mengambil semua data sentimen list berdasarkan provider dan tanggal mulai dan tanggal berakhir

    def get_all_sentimendata_provider_date_to_date(self, provider, tanggal_awal, tanggal_akhir):
        from_str = tanggal_awal
        until_str = tanggal_akhir
        # until_str = "2022-06-18"
        # from_date_str = from_str + "T00:00:00+00:00"
        # to_date_str = until_str + "T23:59:59+00:00"
        # from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        # to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        from_date_str_gmt_7 = from_str + "T00:00:01+07:00"
        to_date_str_gmt_7 = until_str + "T23:59:59+07:00"
        from_date_gmt_7 = datetime.strptime(
            from_date_str_gmt_7, '%Y-%m-%dT%H:%M:%S%z')
        to_date_gmt_7 = datetime.strptime(
            to_date_str_gmt_7, '%Y-%m-%dT%H:%M:%S%z')
        from_date_gmt_0 = from_date_gmt_7.astimezone(
            pytz.timezone('Etc/Greenwich'))
        to_date_gmt_0 = to_date_gmt_7.astimezone(
            pytz.timezone('Etc/Greenwich'))
        print("from")
        print(from_date_gmt_0)
        print("to")
        print(to_date_gmt_0)
        aggregation = [
            {"$match":
                {"$and": [{"created": {"$gte": from_date_gmt_0, "$lte": to_date_gmt_0}}, {
                    "provider": provider}]}
             },
            {
                '$group': {
                    '_id': {
                        'createdAt': {
                            '$dateToString': {
                                'format': '%Y-%m-%d',
                                'date': '$created',
                                'timezone': '+07:00'

                            }
                        },
                        'sentimen': '$sentimen'
                    },
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$group': {
                    '_id': '$_id.createdAt',
                    'products': {
                        '$push': {
                            'sentimen': '$_id.sentimen',
                            'count': '$count'
                        }
                    }
                }
            }, {
                '$replaceRoot': {
                    'newRoot': {
                        '$mergeObjects': [
                            {
                                'created': '$_id'
                            }, {
                                '$arrayToObject': {
                                    '$map': {
                                        'input': '$products',
                                        'in': {
                                            'k': {
                                                '$toString': '$$this.sentimen'
                                            },
                                            'v': '$$this.count'
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            },
            {
                "$sort": {
                    "created": 1
                }

            }, {
                '$project': {
                    'sentimen_pos': {
                        '$ifNull': [
                            '$positif', 0
                        ]
                    },
                    'sentimen_neg': {
                        '$ifNull': [
                            '$negatif', 0
                        ]
                    },
                    'created_date': '$created'
                }
            },

        ]
        cursor = self.tweet_data.aggregate(aggregation)
        sentimen_data = list(cursor)
        sentimen_data = json.loads(json_util.dumps(sentimen_data))
        return sentimen_data

    # def get_all_tweet_with_count_provider_date_to_date(self, provider, tanggal_awal, tanggal_akhir):
    #     from_str = tanggal_awal
    #     until_str = tanggal_akhir
    #     # until_str = "2022-06-18"
    #     from_date_str = from_str + "T00:00:00+00:00"
    #     to_date_str = until_str + "T23:59:59+00:00"
    #     from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
    #     to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
    #     criteria = {"$and": [{"created": {"$gte": from_date, "$lte": to_date}}, {
    #         "provider": provider}]}
    #     aggregation = [
    #         {"$match":
    #             {"$and": [{"created": {"$gte": from_date, "$lte": to_date}}, {
    #                 "provider": provider}]}
    #          },
    #         {
    #             "$facet": {
    #                 "data": [],
    #                 "total": [
    #                     {"$count": "data"},
    #                 ],
    #                 "positive_sentiment": [
    #                     {"$match": {"sentimen": "positif"}},
    #                     {"$count": "data"}
    #                 ],
    #                 "negative_sentiment": [
    #                     {"$match": {"sentimen": "negatif"}},
    #                     {"$count": "data"}
    #                 ],
    #             }
    #         },
    #         {
    #             "$unwind": {"path": "$total"}
    #         },
    #         {
    #             "$unwind": {"path": "$positive_sentiment"}
    #         }, {
    #             "$unwind": {"path": "$negative_sentiment"}
    #         }, {
    #             "$project": {
    #                 "data": 1,
    #                 "count": {
    #                     "total": "$total.data",
    #                     "positive_sentiment": "$positive_sentiment.data",
    #                     "negative_sentiment": "$negative_sentiment.data"
    #                 }
    #             }
    #         },
    #     ]

    #     cursor = self.tweet_data.aggregate(aggregation)
    #     tweet_data = list(cursor)
    #     tweet_data = json.loads(json_util.dumps(tweet_data))
    #     return tweet_data
