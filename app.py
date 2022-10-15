# Library untuk melakukan perhitungan kata
from sklearn.feature_extraction.text import CountVectorizer
# Library untuk membuat REST API
from flask import Flask, jsonify
# Library untuk memperbolehkan CORS
from flask_cors import CORS
# Library untuk memproses JSON
import json
# Library untuk memproses kata
import pandas as pd
# import class yang berisi fungsi pembantu
from tweetdataRepository import TweetDataRepository

# mendefinisikan main
app = Flask(__name__)
# mendefinisikan class
repo = TweetDataRepository()
# memperbolehkan CORS
CORS(app)


@app.route('/api/tweet_w_count/provider/<string:provider_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_tweet_with_count_provider_date_to_date(provider_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_tweet_with_count_provider_date_to_date(provider_pilihan,
                                                                tanggal_awal, tanggal_akhir)
    response = jsonify(todos)
    response.status_code = 200
    return response


@app.route('/api/tweet/provider/<string:provider_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_tweet_saja_provider_date_to_date(provider_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_tweet_saja_provider_date_to_date(provider_pilihan,
                                                          tanggal_awal, tanggal_akhir)
    tweets_df = pd.DataFrame(todos)
    if tweets_df.empty == False:
        tweet_data_json_list = json.loads(json.dumps(
            list(tweets_df.T.to_dict().values())))
        tweets_data_accumulator_json_list = {
            'tweets': tweet_data_json_list
        }
    if tweets_df.empty == True:

        tweet_data_json_list = [{
            "id": 0,
            "created": "-",
            "created_date": "-",
            "created_time": "-",
            "language": "-",
            "username": "-",
            "provider": "-",
            "sentimen": "-",
            "text": "-",
            "text_preprocessed": "-",
            "tweet_id": "-"
        }, ]

        tweets_data_accumulator_json_list = {
            'tweets': tweet_data_json_list
        }

    response = jsonify(tweets_data_accumulator_json_list)
    response.status_code = 200
    return response


@app.route('/api/sentimen/provider/<string:provider_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_sentimendata_provider_date_to_date(provider_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_sentimendata_provider_date_to_date(provider_pilihan,
                                                            tanggal_awal, tanggal_akhir)
    sentimen_daily_df = pd.DataFrame(todos)
    print(sentimen_daily_df)
    if sentimen_daily_df.empty == False:
        sentimen_pos_total = sentimen_daily_df['sentimen_pos'].sum()
        sentimen_neg_total = sentimen_daily_df['sentimen_neg'].sum()
        sentimen_all_total = sentimen_pos_total + sentimen_neg_total
        print(sentimen_pos_total)
        print(sentimen_neg_total)
        print(sentimen_all_total)

        sentimen_total_json_list = {
            'sentimen_all_total': int(sentimen_all_total),
            'sentimen_pos_total': int(sentimen_pos_total),
            'sentimen_neg_total': int(sentimen_neg_total),
        }

        sentimen_daily_df["id"] = sentimen_daily_df.index
        sentimen_daily_df["sentimen_sum"] = sentimen_daily_df["sentimen_neg"] + \
            sentimen_daily_df["sentimen_pos"]
        print(sentimen_daily_df)
        sentimen_daily_json_list = json.loads(json.dumps(
            list(sentimen_daily_df.T.to_dict().values())))

        sentimen_json_list = {
            'sentimen_total': sentimen_total_json_list,
            'sentimen_daily': sentimen_daily_json_list,
        }
    if sentimen_daily_df.empty == True:

        sentimen_total_json_list = {
            'sentimen_all_total': 0,
            'sentimen_pos_total': 0,
            'sentimen_neg_total': 0,
        }
        sentimen_daily_json_list = [{
            'id': 0,
            'created_date': '-',
            'sentimen_pos': 0,
            'sentimen_neg': 0,
            'sentimen_sum': 0,
        }, ]
        sentimen_json_list = {
            'sentimen_total': sentimen_total_json_list,
            'sentimen_daily': sentimen_daily_json_list,
        }

    response = jsonify(sentimen_json_list)
    response.status_code = 200
    return response


@app.route('/api/kata/provider/<string:provider_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_kata_provider_date_to_date(provider_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_tweet_saja_provider_date_to_date(provider_pilihan,
                                                          tanggal_awal, tanggal_akhir)

    tweet_df = pd.DataFrame(todos)
    # tweet_df = pd.DataFrame()
    if tweet_df.empty == False:
        tweet_positif_df = tweet_df[tweet_df["sentimen"].str.contains(
            "positif") == True]
        if tweet_positif_df.empty == False:
            word_vectorizer_positif = CountVectorizer(ngram_range=(1, 2))
            sparse_matrix_positif = word_vectorizer_positif.fit_transform(
                tweet_positif_df['text_preprocessed'])
            frequencies_positif = sum(sparse_matrix_positif).toarray()[
                0]  # [0] adalah axis 0 yaitu column
            frequencies_positif_df = pd.DataFrame(frequencies_positif, index=word_vectorizer_positif.get_feature_names_out(),
                                                  columns=['frequency'])
            frequencies_positif_df = frequencies_positif_df.sort_values(
                by=['frequency'], ascending=False)
            frequencies_positif_df = frequencies_positif_df.head(30)
            frequencies_positif_pasangan = list(
                zip(frequencies_positif_df.index, frequencies_positif_df["frequency"]))
            frequencies_positif_pasangan_df = pd.DataFrame(frequencies_positif_pasangan,
                                                           columns=['kata', 'frekuensi'])
            print(frequencies_positif_pasangan_df)
            frequencies_positif_pasangan_df["id"] = frequencies_positif_pasangan_df.index
            print(frequencies_positif_pasangan_df)
            kata_positif_json_list = json.loads(json.dumps(
                list(frequencies_positif_pasangan_df.T.to_dict().values())))

        if tweet_positif_df.empty == True:
            kata_positif_json_list = {
                'kata': '-',
                'frekuensi': 0,
                'id': 0,
            }

        tweet_negatif_df = tweet_df[tweet_df["sentimen"].str.contains(
            "negatif") == True]

        if tweet_negatif_df.empty == False:
            word_vectorizer_negatif = CountVectorizer(ngram_range=(1, 2))
            sparse_matrix_negatif = word_vectorizer_negatif.fit_transform(
                tweet_negatif_df['text_preprocessed'])
            frequencies_negatif = sum(sparse_matrix_negatif).toarray()[
                0]  # [0] adalah axis 0 yaitu column
            frequencies_negatif_df = pd.DataFrame(frequencies_negatif, index=word_vectorizer_negatif.get_feature_names_out(),
                                                  columns=['frequency'])
            frequencies_negatif_df = frequencies_negatif_df.sort_values(
                by=['frequency'], ascending=False)
            frequencies_negatif_df = frequencies_negatif_df.head(30)
            frequencies_negatif_pasangan = list(
                zip(frequencies_negatif_df.index, frequencies_negatif_df["frequency"]))
            frequencies_negatif_pasangan_df = pd.DataFrame(frequencies_negatif_pasangan,
                                                           columns=['kata', 'frekuensi'])
            print(frequencies_negatif_pasangan_df)
            frequencies_negatif_pasangan_df["id"] = frequencies_negatif_pasangan_df.index
            print(frequencies_negatif_pasangan_df)
            kata_negatif_json_list = json.loads(json.dumps(
                list(frequencies_negatif_pasangan_df.T.to_dict().values())))

        if tweet_negatif_df.empty == True:
            kata_negatif_json_list = {
                'kata': '-',
                'frekuensi': 0,
                'id': 0,
            }
        # ----------------- kudu ning ngisor mungkin
        data_kata = {
            'kata_positif': kata_positif_json_list,
            'kata_negatif': kata_negatif_json_list
        }
    if tweet_df.empty == True:
        kata_negatif_json_list = [{
            'kata': '-',
            'frekuensi': 0,
            'id': 0,
        }, ]
        kata_positif_json_list = [{
            'kata': '-',
            'frekuensi': 0,
            'id': 0,
        }, ]
        data_kata = {
            'kata_positif': kata_positif_json_list,
            'kata_negatif': kata_negatif_json_list
        }

    response = jsonify(data_kata)
    response.status_code = 200
    return response


if __name__ == '__main__':
    app.run(debug=True)
