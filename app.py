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
import numpy as np

# mendefinisikan main
app = Flask(__name__)
# mendefinisikan class
repo = TweetDataRepository()
# memperbolehkan CORS
CORS(app)


def _hapus_spasi(input_text):
    # The split() method splits a string into a list.
    # default separator is any whitespace.
    # "internet lambat    banget" -----> ['internet', 'lambat', 'banget']
    words = input_text.split()
    # The join() method takes all items in an iterable and joins them into one string.
    # A string must be specified as the separator.
    # " " <----------- seperator
    # ['internet', 'lambat', 'banget'] ---------> "internet lambat banget"
    new_text = " ".join(words)
    # Mengembalikan nilai new_text
    return new_text


@app.route('/api/fix_tweet/provider/<string:provider_pilihan>/lokasi/<string:lokasi_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_fix_tweet_provider_lokasi_date_to_date(provider_pilihan, lokasi_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_tweet_provider_lokasi_date_to_date(provider_pilihan, lokasi_pilihan,
                                                            tanggal_awal, tanggal_akhir)
    tweets_df = pd.DataFrame(todos)
    print(tweets_df)
    if tweets_df.empty == False:
        # seleksi lagi
        tweets_df['text_preprocessed_2'] = tweets_df['text'].str.lower()
        tweets_df["text_preprocessed_2"] = tweets_df["text_preprocessed_2"].str.replace(
            '\n', ' ')
        tweets_df['text_preprocessed_2'] = tweets_df['text_preprocessed_2'].str.replace(
            'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ')
        tweets_df['text_preprocessed_2'] = tweets_df['text_preprocessed_2'].apply(
            _hapus_spasi)
        print(tweets_df)
        tweets_df = tweets_df.drop_duplicates(subset='text_preprocessed_2')
        tweets_df = tweets_df.sort_values(by=['created'])
        tweets_df = tweets_df.reset_index(drop=True)
        print(tweets_df)
        print(tweets_df.columns)
        tweets_df.drop(tweets_df.columns[13], axis=1, inplace=True)
        print(tweets_df.columns)
        # ------------------------------
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


@app.route('/api/fix_kata/provider/<string:provider_pilihan>/lokasi/<string:lokasi_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_fix_kata_lokasi_provider_date_to_date(provider_pilihan, lokasi_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_tweet_provider_lokasi_date_to_date(provider_pilihan, lokasi_pilihan,
                                                            tanggal_awal, tanggal_akhir)

    tweets_df = pd.DataFrame(todos)
    # tweets_df = pd.DataFrame()
    if tweets_df.empty == False:

        # seleksi lagi
        tweets_df['text_preprocessed_2'] = tweets_df['text'].str.lower()
        tweets_df["text_preprocessed_2"] = tweets_df["text_preprocessed_2"].str.replace(
            '\n', ' ')
        tweets_df['text_preprocessed_2'] = tweets_df['text_preprocessed_2'].str.replace(
            'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ')
        tweets_df['text_preprocessed_2'] = tweets_df['text_preprocessed_2'].apply(
            _hapus_spasi)
        print(tweets_df)
        tweets_df = tweets_df.drop_duplicates(subset='text_preprocessed_2')
        tweets_df = tweets_df.reset_index(drop=True)
        tweets_df.drop(tweets_df.columns[13], axis=1, inplace=True)
        print(tweets_df.columns)
        # ------------------------------

        tweet_positif_df = tweets_df[tweets_df["sentimen"].str.contains(
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
            frequencies_positif_df = frequencies_positif_df.head(50)
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

        tweet_negatif_df = tweets_df[tweets_df["sentimen"].str.contains(
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
            frequencies_negatif_df = frequencies_negatif_df.head(50)
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
    if tweets_df.empty == True:
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


@app.route('/api/fix_sentimen/provider/<string:provider_pilihan>/lokasi/<string:lokasi_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_fix_sentimen_provider_lokasi_date_to_date(provider_pilihan, lokasi_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_tweet_provider_lokasi_date_to_date(provider_pilihan, lokasi_pilihan,
                                                            tanggal_awal, tanggal_akhir)
    tweets_df = pd.DataFrame(todos)
    if tweets_df.empty == False:
        # ------------
        tweets_df['text_preprocessed_2'] = tweets_df['text'].str.lower()
        tweets_df["text_preprocessed_2"] = tweets_df["text_preprocessed_2"].str.replace(
            '\n', ' ')
        tweets_df['text_preprocessed_2'] = tweets_df['text_preprocessed_2'].str.replace(
            'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ')
        tweets_df['text_preprocessed_2'] = tweets_df['text_preprocessed_2'].apply(
            _hapus_spasi)
        # print(tweets_df)
        tweets_df = tweets_df.drop_duplicates(subset='text_preprocessed_2')
        tweets_df = tweets_df.reset_index(drop=True)
        tweets_df.drop(tweets_df.columns[13], axis=1, inplace=True)
        # ------------

        tanggal_var = tweets_df['created_date'].value_counts()
        tanggal_var_df = pd.DataFrame(tanggal_var)
        tanggal_var = tanggal_var_df.index.to_numpy()
        tanggal_var = np.sort(tanggal_var, axis=None)

        sentimen_positif = []
        sentimen_negatif = []
        sentimen_total = []

        for i in tanggal_var:
            print("--------------------")
            print(i)
            # name = i
            # xx = str(name)
            df_name = "df" + str(i)
            df_name = tweets_df[tweets_df["created_date"].str.contains(
                i) == True]
            print(df_name)
            print("--------------------")
            count_sentimen_pos = 0
            count_sentimen_neg = 0
            for index, row in df_name.iterrows():
                text = row["sentimen"]
                if text == "positif":
                    count_sentimen_pos = count_sentimen_pos + 1
                else:
                    count_sentimen_neg = count_sentimen_neg + 1
            count_sentimen_tot = count_sentimen_pos + count_sentimen_neg
            sentimen_positif.append(count_sentimen_pos)
            sentimen_negatif.append(count_sentimen_neg)
            sentimen_total.append(count_sentimen_tot)

        print(tanggal_var)
        print(sentimen_positif)
        print(sentimen_negatif)
        print(sentimen_total)

        df_sentimen = pd.DataFrame(
            columns=['created_date', 'sentimen_pos', 'sentimen_neg', 'sentimen_sum', 'id'])
        df_sentimen['created_date'] = tanggal_var
        df_sentimen['sentimen_pos'] = sentimen_positif
        df_sentimen['sentimen_neg'] = sentimen_negatif
        df_sentimen['sentimen_sum'] = sentimen_total
        df_sentimen['id'] = df_sentimen.index

        df_sentimen['sentimen_pos_persen'] = (df_sentimen['sentimen_pos'] /
                                              df_sentimen['sentimen_sum']) * 100
        df_sentimen['sentimen_neg_persen'] = (df_sentimen['sentimen_neg'] /
                                              df_sentimen['sentimen_sum']) * 100
        df_sentimen["sentimen_pos_persen"] = df_sentimen["sentimen_pos_persen"].round(
            1)
        df_sentimen["sentimen_neg_persen"] = df_sentimen["sentimen_neg_persen"].round(
            1)

        sentimen_pos_total = df_sentimen['sentimen_pos'].sum()
        sentimen_neg_total = df_sentimen['sentimen_neg'].sum()
        sentimen_all_total = sentimen_pos_total + sentimen_neg_total
        sentimen_pos_total_persen = (
            (sentimen_pos_total / sentimen_all_total) * 100).round(1)
        sentimen_neg_total_persen = (
            (sentimen_neg_total / sentimen_all_total) * 100).round(1)
        print(sentimen_pos_total)
        print(sentimen_neg_total)
        print(sentimen_all_total)
        print(sentimen_pos_total_persen)
        print(sentimen_neg_total_persen)
        print(df_sentimen)

        df_sentimen_kosongan = pd.DataFrame()
        df_sentimen_kosongan["created_date"] = pd.date_range(
            start=tanggal_awal, end=tanggal_akhir)
        df_sentimen_kosongan['sentimen_pos'] = 0
        df_sentimen_kosongan['sentimen_neg'] = 0
        df_sentimen_kosongan['sentimen_sum'] = 0
        df_sentimen_kosongan['id'] = 0
        df_sentimen_kosongan['sentimen_pos_persen'] = 0
        df_sentimen_kosongan['sentimen_neg_persen'] = 0

        df_sentimen_kosongan["created_date"] = pd.to_datetime(
            df_sentimen_kosongan["created_date"], format="%Y/%m/%d")
        df_sentimen["created_date"] = pd.to_datetime(
            df_sentimen["created_date"], format="%Y/%m/%d")

        agg_functions = {
            'sentimen_pos': 'sum',
            'sentimen_neg': 'sum',
            'sentimen_sum': 'sum',
            'id': 'sum',
            'sentimen_pos_persen': 'sum',
            'sentimen_neg_persen': 'sum',
        }

        df_sentimen_gabungan = pd.concat([df_sentimen, df_sentimen_kosongan
                                          ])
        df_sentimen_gabungan = df_sentimen_gabungan.reset_index(drop=True)
        # create new DataFrame by combining rows with same id values
        df_sentimen_gabungan = df_sentimen_gabungan.groupby(
            df_sentimen_gabungan['created_date']).aggregate(agg_functions)
        df_sentimen_gabungan["created_datee"] = df_sentimen_gabungan.index
        # print(df_sentimen_kosongan)
        print(df_sentimen_gabungan)
        # print(df_sentimen_gabungan.dtypes)
        print(df_sentimen_kosongan.dtypes)
        print(df_sentimen.dtypes)
        print(df_sentimen_gabungan.dtypes)

        df_sentimen = pd.DataFrame()
        df_sentimen["created_date"] = df_sentimen_gabungan['created_datee']
        df_sentimen['sentimen_pos'] = df_sentimen_gabungan['sentimen_pos']
        df_sentimen['sentimen_neg'] = df_sentimen_gabungan['sentimen_neg']
        df_sentimen['sentimen_sum'] = df_sentimen_gabungan['sentimen_sum']
        df_sentimen['id'] = df_sentimen_kosongan.index
        df_sentimen['sentimen_pos_persen'] = df_sentimen_gabungan['sentimen_pos_persen']
        df_sentimen['sentimen_neg_persen'] = df_sentimen_gabungan['sentimen_neg_persen']
        df_sentimen = df_sentimen.reset_index(drop=True)
        df_sentimen["created_date"] = df_sentimen["created_date"].apply(str)
        print(df_sentimen)
        print(df_sentimen.dtypes)

        kantong_tanggal = []
        for index, row in df_sentimen.iterrows():
            tanggal_dengan_jam = row["created_date"]
            tanggal_saja = tanggal_dengan_jam[0:10]
            kantong_tanggal.append(tanggal_saja)

        df_sentimen["created_date"] = kantong_tanggal

        sentimen_total_json_list = {
            'sentimen_all_total': int(sentimen_all_total),
            'sentimen_pos_total': int(sentimen_pos_total),
            'sentimen_neg_total': int(sentimen_neg_total),
            'sentimen_pos_total_persen': float(sentimen_pos_total_persen),
            'sentimen_neg_total_persen': float(sentimen_neg_total_persen),
        }

        sentimen_daily_json_list = json.loads(json.dumps(
            list(df_sentimen.T.to_dict().values())))

        sentimen_json_list = {
            'sentimen_total': sentimen_total_json_list,
            'sentimen_daily': sentimen_daily_json_list,
        }

    if tweets_df.empty == True:

        sentimen_total_json_list = {
            'sentimen_all_total': 0,
            'sentimen_pos_total': 0,
            'sentimen_neg_total': 0,
            'sentimen_pos_total_persen': 0,
            'sentimen_neg_total_persen': 0,
        }
        sentimen_daily_json_list = [{
            'id': 0,
            'created_date': '-',
            'sentimen_pos': 0,
            'sentimen_neg': 0,
            'sentimen_sum': 0,
            'sentimen_pos_persen': 0,
            'sentimen_neg_persen': 0,
        }, ]
        sentimen_json_list = {
            'sentimen_total': sentimen_total_json_list,
            'sentimen_daily': sentimen_daily_json_list,
        }

    response = jsonify(sentimen_json_list)
    response.status_code = 200
    return response


if __name__ == '__main__':
    app.run(debug=True)
