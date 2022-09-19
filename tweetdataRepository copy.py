from pymongo import MongoClient
from bson import json_util, ObjectId
import json
from datetime import datetime


class TweetDataRepository:

    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.TwitterStream
        self.tweet_data = self.db.tweetsSentiment2
        self.kata_data = self.db.tweetsWord2
        self.sentimen_data = self.db.tweetsSentimentList2

    # mengambil semua data tweet

    def get_all_tweetdata(self):
        cursor = self.tweet_data.find({})
        tweet_data = list(cursor)
        tweet_data = json.loads(json_util.dumps(tweet_data))
        for item in tweet_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
        return tweet_data

    # mengambil semua data kata
    #

    def get_all_katadata(self):
        cursor = self.kata_data.find({})
        kata_data = list(cursor)
        kata_data = json.loads(json_util.dumps(kata_data))
        for item in kata_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
            item["kata"] = item["kata"][0]
        return kata_data

    # mengambil semua data tweet berdasarkan provider

    def get_all_tweetdata_provider(self, provider):
        cursor = self.tweet_data.find({"provider": provider})
        tweet_data = list(cursor)
        tweet_data = json.loads(json_util.dumps(tweet_data))
        for item in tweet_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
        return tweet_data

    # mengambil semua data tweet berdasarkan provider

    def get_all_katadata_provider(self, provider):
        cursor = self.kata_data.find({"provider": provider})
        kata_data = list(cursor)
        kata_data = json.loads(json_util.dumps(kata_data))
        for item in kata_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
        return kata_data

    # mengambil semua data tweet berdasarkan provider dan tanggal

    def get_all_tweetdata_provider_date(self, provider, tanggal):
        from_str = tanggal
        until_str = tanggal
        # until_str = "2022-06-18"
        from_date_str = from_str + "T00:00:00+00:00"
        to_date_str = until_str + "T23:59:59+00:00"
        from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        criteria = {"$and": [{"created": {"$gte": from_date, "$lte": to_date}}, {
            "provider": provider}]}
        cursor = self.tweet_data.find(criteria)
        tweet_data = list(cursor)
        tweet_data = json.loads(json_util.dumps(tweet_data))

        for item in tweet_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
        return tweet_data

    # mengambil semua data kata berdasarkan provider dan tanggal
    #

    def get_all_katadata_provider_date(self, provider, tanggal):
        from_str = tanggal
        until_str = tanggal
        # until_str = "2022-06-18"
        from_date_str = from_str + "T00:00:00+00:00"
        to_date_str = until_str + "T23:59:59+00:00"
        from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        criteria = {"$and": [{"created": {"$gte": from_date, "$lte": to_date}}, {
            "provider": provider}]}
        cursor = self.kata_data.find(criteria)
        kata_data = list(cursor)
        kata_data = json.loads(json_util.dumps(kata_data))
        for item in kata_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
            item["kata"] = item["kata"][0]
        return kata_data

    # mengambil semua data tweet berdasarkan provider dan tanggal mulai dan tanggal berakhir

    def get_all_tweetdata_provider_date_to_date(self, provider, tanggal_awal, tanggal_akhir):
        from_str = tanggal_awal
        until_str = tanggal_akhir
        # until_str = "2022-06-18"
        from_date_str = from_str + "T00:00:00+00:00"
        to_date_str = until_str + "T23:59:59+00:00"
        from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        criteria = {"$and": [{"created": {"$gte": from_date, "$lte": to_date}}, {
            "provider": provider}]}
        aggregation = [
            {"$match":
                {"$and": [{"created": {"$gte": from_date, "$lte": to_date}}, {
                    "provider": provider}]}
             },
            {
                "$facet": {
                    "data": [],
                    "total": [
                        {"$count": "data"},
                    ],
                    "positive_sentiment": [
                        {"$match": {"sentimen": "positif"}},
                        {"$count": "data"}
                    ],
                    "negative_sentiment": [
                        {"$match": {"sentimen": "negatif"}},
                        {"$count": "data"}
                    ],
                }
            },
            {
                "$unwind": {"path": "$total"}
            },
            {
                "$unwind": {"path": "$positive_sentiment"}
            }, {
                "$unwind": {"path": "$negative_sentiment"}
            }, {
                "$project": {
                    "data": 1,
                    "count": {
                        "total": "$total.data",
                        "positive_sentiment": "$positive_sentiment.data",
                        "negative_sentiment": "$negative_sentiment.data"
                    }
                }
            },
        ]
        # cursor = self.tweet_data.aggregate(aggregation)
        # tweet_data = list(cursor)
        # print(tweet_data)
        # if tweet_data:
        #     # tweet_data = list(cursor)
        #     tweet_data = json.loads(json_util.dumps(tweet_data))
        #     tweet_data = tweet_data[0]
        #     for item in tweet_data["data"]:
        #         item["_id"] = item["_id"]["$oid"]
        #         item["created"] = item["created"]["$date"]
        #     return tweet_data
        # else:
        #     return tweet_data
        # cursor = self.tweet_data.aggregate(aggregation)

        # tweet_data = list(cursor)
        # tweet_data = json.loads(json_util.dumps(tweet_data))
        # tweet_data = tweet_data[0]
        # for item in tweet_data["data"]:
        #     item["_id"] = item["_id"]["$oid"]
        #     item["created"] = item["created"]["$date"]
        # return tweet_data
        cursor = self.tweet_data.find(criteria)
        tweet_data = list(cursor)
        tweet_data = json.loads(json_util.dumps(tweet_data))
        # tweet_data = tweet_data[0]
        for item in tweet_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
        return tweet_data

    # mengambil semua data kata berdasarkan provider dan tanggal mulai dan tanggal berakhir
    #

    def get_all_katadata_provider_date_to_date(self, provider, tanggal_awal, tanggal_akhir):
        from_str = tanggal_awal
        until_str = tanggal_akhir
        # until_str = "2022-06-18"
        from_date_str = from_str + "T00:00:00+00:00"
        to_date_str = until_str + "T23:59:59+00:00"
        from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        criteria = {"$and": [{"created": {"$gte": from_date, "$lte": to_date}}, {
            "provider": provider}]}
        cursor = self.kata_data.find(criteria)
        kata_data = list(cursor)
        kata_data = json.loads(json_util.dumps(kata_data))
        for item in kata_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
            item["kata"] = item["kata"][0]
        return kata_data

    #  mengambil semua data sentimen list berdasarkan provider dan tanggal mulai dan tanggal berakhir
    #
    #

    def get_all_sentimendata_provider_date_to_date(self, provider, tanggal_awal, tanggal_akhir):
        from_str = tanggal_awal
        until_str = tanggal_akhir
        # until_str = "2022-06-18"
        from_date_str = from_str + "T00:00:00+00:00"
        to_date_str = until_str + "T23:59:59+00:00"
        from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        criteria = {"$and": [{"created": {"$gte": from_date, "$lte": to_date}}, {
            "provider": provider}]}
        aggregation = [
            {"$match":
                {"$and": [{"created": {"$gte": from_date, "$lte": to_date}}, {
                    "provider": provider}]}
             },
            {
                '$group': {
                    '_id': {
                        'createdAt': {
                            '$dateToString': {
                                'format': '%Y-%m-%d',
                                'date': '$created'
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
                                '_id': '$_id'
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
                    "_id": 1
                }

            }

        ]
        # cursor = self.tweet_data.aggregate(aggregation)
        cursor = self.sentimen_data.find(criteria)
        sentimen_data = list(cursor)
        sentimen_data = json.loads(json_util.dumps(sentimen_data))
        for item in sentimen_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
            # item["kata"] = item["kata"][0]
        return sentimen_data

    # mengambil semua data tweet/ berdasasr tanggal

    def get_all_tweetdata_date(self, tanggal):
        from_str = tanggal
        until_str = tanggal
        # until_str = "2022-06-18"
        from_date_str = from_str + "T00:00:00+00:00"
        to_date_str = until_str + "T23:59:59+00:00"
        from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        cursor = self.tweet_data.find({"created": {
            "$gte": from_date, "$lte": to_date}})
        tweet_data = list(cursor)
        tweet_data = json.loads(json_util.dumps(tweet_data))
        for item in tweet_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
        return

    # mengambil semua data kata/ berdasarkan tanggal
    #

    def get_all_katadata_date(self, tanggal):
        from_str = tanggal
        until_str = tanggal
        # until_str = "2022-06-18"
        from_date_str = from_str + "T00:00:00+00:00"
        to_date_str = until_str + "T23:59:59+00:00"
        from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        cursor = self.kata_data.find({"created": {
            "$gte": from_date, "$lte": to_date}})
        kata_data = list(cursor)
        kata_data = json.loads(json_util.dumps(kata_data))
        for item in kata_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
            item["kata"] = item["kata"][0]
        return kata_data

    # mengambil semua data tweet/ berdasarkan tanggal mulai dan tanggal berakhir

    def get_all_tweetdata_date_to_date(self, dari_tanggal, sampai_tanggal):
        from_str = dari_tanggal
        until_str = sampai_tanggal
        # until_str = "2022-06-18"
        from_date_str = from_str + "T00:00:00+00:00"
        to_date_str = until_str + "T23:59:59+00:00"
        from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        cursor = self.tweet_data.find({"created": {
            "$gte": from_date, "$lte": to_date}})
        tweet_data = list(cursor)
        tweet_data = json.loads(json_util.dumps(tweet_data))
        for item in tweet_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
        return tweet_data

    # mengambil semua data kata/ berdasarkan tanggal mulai dan tanggal berakhir
    #

    def get_all_katadata_date_to_date(self, dari_tanggal, sampai_tanggal):
        from_str = dari_tanggal
        until_str = sampai_tanggal
        # until_str = "2022-06-18"
        from_date_str = from_str + "T00:00:00+00:00"
        to_date_str = until_str + "T23:59:59+00:00"
        from_date = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%S%z')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%S%z')
        cursor = self.kata_data.find({"created": {
            "$gte": from_date, "$lte": to_date}})
        kata_data = list(cursor)
        kata_data = json.loads(json_util.dumps(kata_data))
        for item in kata_data:
            item["_id"] = item["_id"]["$oid"]
            item["created"] = item["created"]["$date"]
            item["kata"] = item["kata"][0]
        return kata_data
