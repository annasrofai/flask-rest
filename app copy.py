from sklearn.feature_extraction.text import CountVectorizer
from flask import Flask, jsonify
from tweetdataRepository import TweetDataRepository
from flask_cors import CORS
import pandas as pd
import json
app = Flask(__name__)
repo = TweetDataRepository()
CORS(app)

# mengambil semua data tweet


@app.route('/api/tweet', methods=['GET'])
def get_all_tweetdata():
    tweet_data = repo.get_all_tweetdata()
    response = jsonify(tweet_data)
    response.status_code = 200
    return response

# mengambil semua data kata
#


@app.route('/api/kata', methods=['GET'])
def get_all_katadata():
    tweet_data = repo.get_all_katadata()
    response = jsonify(tweet_data)
    response.status_code = 200
    return response


# mengambil semua data tweet berdasarkan provider


@app.route('/api/tweet/provider/<string:provider_pilihan>', methods=['GET'])
def get_all_tweetdata_provider(provider_pilihan):
    todos = repo.get_all_tweetdata_provider(provider_pilihan)
    response = jsonify(todos)
    response.status_code = 200
    return response

# mengambil semua data tweet berdasarkan provider
#


@app.route('/api/kata/provider/<string:provider_pilihan>', methods=['GET'])
def get_all_katadata_provider(provider_pilihan):
    todos = repo.get_all_katadata_provider(provider_pilihan)
    response = jsonify(todos)
    response.status_code = 200
    return response


# mengambil semua data tweet berdasarkan provider dan tanggal


@app.route('/api/tweet/provider/<string:provider_pilihan>/<string:tanggal>', methods=['GET'])
def get_all_tweetdata_provider_date(provider_pilihan, tanggal):
    todos = repo.get_all_tweetdata_provider_date(provider_pilihan, tanggal)
    response = jsonify(todos)
    response.status_code = 200
    return response

# mengambil semua data tweet berdasarkan provider dan tanggal
#


@app.route('/api/kata/provider/<string:provider_pilihan>/<string:tanggal>', methods=['GET'])
def get_all_katadata_provider_date(provider_pilihan, tanggal):
    todos = repo.get_all_katadata_provider_date(provider_pilihan, tanggal)
    response = jsonify(todos)
    response.status_code = 200
    return response

# mengambil semua data tweet berdasarkan provider dan tanggal mulai dan tanggal berakhir


@app.route('/api/tweet/provider/<string:provider_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_tweetdata_provider_date_to_date(provider_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_tweetdata_provider_date_to_date(provider_pilihan,
                                                         tanggal_awal, tanggal_akhir)
    response = jsonify(todos)
    response.status_code = 200
    return response


# mengambil semua data kata berdasarkan provider dan tanggal mulai dan tanggal berakhir
#


@app.route('/api/kata/provider/<string:provider_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_katadata_provider_date_to_date(provider_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_katadata_provider_date_to_date(provider_pilihan,
                                                        tanggal_awal, tanggal_akhir)
    # aa = "dasdasdas"
    # if todos != "aaa":
    #     response = jsonify(todos)
    #     response.status_code = 200
    #     return response
    # return aa
    response = jsonify(todos)
    response.status_code = 200
    return response

# mengambil semua data kata berdasarkan provider dan tanggal mulai dan tanggal berakhir
#


@app.route('/api/sentimen/provider/<string:provider_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_sentimendata_provider_date_to_date(provider_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_sentimendata_provider_date_to_date(provider_pilihan,
                                                            tanggal_awal, tanggal_akhir)
    # aa = "dasdasdas"
    # if todos != "aaa":
    #     response = jsonify(todos)
    #     response.status_code = 200
    #     return response
    # return aa
    response = jsonify(todos)
    response.status_code = 200
    return response


# mengambil semua data tweet/ berdasasr tanggal


@app.route('/api/tweet/tanggal/<string:tanggal>', methods=['GET'])
def get_all_tweetdata_date(tanggal):
    todos = repo.get_all_tweetdata_date(tanggal)
    response = jsonify(todos)
    response.status_code = 200
    return response

# mengambil semua data tweet/ berdasasr tanggal
#


@app.route('/api/kata/tanggal/<string:tanggal>', methods=['GET'])
def get_all_katadata_date(tanggal):
    todos = repo.get_all_katadata_date(tanggal)
    response = jsonify(todos)
    response.status_code = 200
    return response

# mengambil semua data tweet/ berdasarkan tanggal mulai dan tanggal berakhir


@app.route('/api/tweet/tanggal/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_tweetdata_date_to_date(tanggal_awal, tanggal_akhir):
    todos = repo.get_all_tweetdata_date_to_date(tanggal_awal, tanggal_akhir)
    response = jsonify(todos)
    response.status_code = 200
    return response

# mengambil semua data kata/ berdasarkan tanggal mulai dan tanggal berakhir
#


@app.route('/api/kata/tanggal/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_katadata_date_to_date(tanggal_awal, tanggal_akhir):
    todos = repo.get_all_katadata_date_to_date(tanggal_awal, tanggal_akhir)
    response = jsonify(todos)
    response.status_code = 200
    return response


@app.route('/api/coba/provider/<string:provider_pilihan>/periode/<string:tanggal_awal>/<string:tanggal_akhir>', methods=['GET'])
def get_all_coba_provider_date_to_date(provider_pilihan, tanggal_awal, tanggal_akhir):
    todos = repo.get_all_tweetdata_provider_date_to_date(provider_pilihan,
                                                         tanggal_awal, tanggal_akhir)
    # yy = pd.DataFrame(todos)
    print(todos)
    yy = pd.DataFrame(todos)
    print(yy)

    word_vectorizer_indihome = CountVectorizer(ngram_range=(1, 2))

    sparse_matrix_indihome = word_vectorizer_indihome.fit_transform(
        yy['text_preprocessed'])
    tabel_indihome_df = pd.DataFrame(sparse_matrix_indihome.toarray(
    ), index=yy["text_preprocessed"], columns=word_vectorizer_indihome.get_feature_names_out())
    frequencies_indihome = sum(sparse_matrix_indihome).toarray()[
        0]  # [0] adalah axis 0 yaitu column
    frequencies_indihome_df = pd.DataFrame(frequencies_indihome, index=word_vectorizer_indihome.get_feature_names_out(),
                                           columns=['frequency'])
    frequencies_indihome_df = frequencies_indihome_df.sort_values(
        by=['frequency'], ascending=False)
    # frequencies_indihome_df["frequency"] = frequencies_indihome_df["frequency"].apply(
    #     str)
    frequencies_indihome_df = frequencies_indihome_df.head(20)
    frequencies_indihome_pasangan = list(
        zip(frequencies_indihome_df.index, frequencies_indihome_df["frequency"]))
    aa = pd.DataFrame(frequencies_indihome_pasangan,
                      columns=['kata', 'frekuensi'])
    # frequencies_indihome_dict = dict(frequencies_indihome_pasangan)
    # print(frequencies_dict)

    json_list = json.loads(json.dumps(list(aa.T.to_dict().values())))
    print(frequencies_indihome_df)
    print(frequencies_indihome_pasangan)
    print(aa)
    print(json_list)
    # print(todos)
    # print(yy)
    # aa = yy.to_json(orient="table")
    # json_list = json.loads(json.dumps(list(yy.T.to_dict().values())))
    response = jsonify(json_list)
    response.status_code = 200
    return response


if __name__ == '__main__':
    app.run(debug=True)
