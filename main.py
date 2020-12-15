from flask import Flask
from flask_cors import CORS
from waitress import serve
import json
import numpy as np
from sklearn.cluster import KMeans
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import pandas as pd

app = Flask(__name__)
CORS(app)


@app.route('/api/clustering', methods=['GET'])
def hello():
    data = k_means_clustering()
    return json.dumps(data)


@app.route('/api/class/<id>/clustering', methods=['GET'])
def helloClassClustering(id):
    data = k_means_clustering_class_wise(id)
    return json.dumps(data)


def get_con():
    con_parms = {
        'host': 'localhost',
        'port': '3306',
        'database': 'lms_db',
        'user': 'root',
        'password': ""
    }

    target_engine = create_engine(
        """mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}""".format(**con_parms),
        poolclass=QueuePool, pool_size=10,
        echo=False, pool_pre_ping=True)
    return target_engine


def k_means_clustering():
    con = get_con()
    query = """select sum(history.no_views) as view_count,sum(history.time_spent) as time_spent, users.first_name, users.last_name, users.u_id, history.user_id from history join users on history.user_id = users.id where users.role = "user" AND history.is_deleted = 0 group by history.user_id order by 1,2;"""
    data_df = pd.read_sql(query, con)
    if len(data_df) < 4:
        return []
    X = np.column_stack((data_df["view_count"], data_df["time_spent"]))
    kmeans = KMeans(n_clusters=4, init='k-means++', max_iter=300, n_init=10,
                    random_state=0)
    pred_y = kmeans.fit_predict(X)
    mapping = {0: "worst", 1: "best", 2: "close to best", 3: "close to worst"}
    res = []
    for i in range(0, len(data_df)):
        res.append({"user_id": int(data_df["user_id"][i]), "first_name": data_df["first_name"][i], "last_name": data_df["last_name"][i], "u_id": data_df["u_id"][i],"category": mapping[pred_y[i]]})
    return res


def k_means_clustering_class_wise(id):
    con = get_con()
    qparam = { 'id': id }
    query = """select sum(history.no_views) as view_count,sum(history.time_spent) as time_spent, users.first_name, users.last_name, users.u_id, history.user_id from history join users on history.user_id = users.id where users.role = "user" AND history.class_id = {id} AND history.is_deleted = 0 group by history.user_id order by 1,2;""".format(**qparam)
    data_df = pd.read_sql(query, con)
    if len(data_df) < 4:
        return []
    X = np.column_stack((data_df["view_count"], data_df["time_spent"]))
    kmeans = KMeans(n_clusters=4, init='k-means++', max_iter=300, n_init=10,
                    random_state=0)
    pred_y = kmeans.fit_predict(X)
    mapping = {0: "worst", 1: "best", 2: "close to best", 3: "close to worst"}
    res = []
    for i in range(0, len(data_df)):
        res.append({"user_id": int(data_df["user_id"][i]), "first_name": data_df["first_name"][i], "last_name": data_df["last_name"][i], "u_id": data_df["u_id"][i], "category": mapping[pred_y[i]]})
    return res


if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=4000)
