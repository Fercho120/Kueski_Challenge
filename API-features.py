#-*- coding: utf-8 -*-
import os
from joblib import load
import sqlite3
from flask import Flask, request, jsonify
 
def sql_fetch(con, consulta):
    cursorObj = con.cursor()
    cursorObj.execute("SELECT age, years_on_the_job, nb_previous_loans, avg_amount_loans_previous, flag_own_car FROM features WHERE id = '%s'" % consulta)
    features = cursorObj.fetchall()
    return features
 
app = Flask(__name__)
 
@app.route('/', methods=['POST'])
def makecalc():
    con = sqlite3.connect('features.db')
    data = request.get_json()
    features = sql_fetch(con, data.get("id"))
    print("Esto son los features", features)
    result = model.predict(features)
    print("Este es el resultado de la preddiccion", int(result))
    return jsonify(int(result))
 
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5010))
    model = load('model_risk.joblib') 
    print("Starting app on port %d" %port)
    app.run(debug=True, port=port, host="0.0.0.0")
