from flask import Flask, render_template, request
import pandas as pd
import time
import ElasticSearch,Variables
app = Flask(__name__)


@app.route("/")
def home():
    return render_template("home.html")

@app.route("/get")
def get_bot_response():
    userText = request.args.get('msg')

    resumes=main(userText.split())
    # print(userText.split())
    return str(resumes)

def main(query):
    query_skills=query[0]+""
    query_years_of_exp = float(query[1])
    query_job_type = query[2] + ""

    res1=ElasticSearch.results(Variables.IndexedFiles,query_skills,query_years_of_exp,query_job_type)
    Resumes=[]
    print(res1['hits']['hits'])
    for item in res1['hits']['hits']:
        if bool(item['_source']) is True:
            if item['_source']['Filename'] not in Resumes:
                Resumes.append(item['_source']['Filename'])

    print("Resumes",Resumes)
    print("Total resumes",len(Resumes))
    return Resumes


@app.before_first_request
def before_first_request():
    print('########### started Loading USE')
    start = time.time()
    path = "/home/manjunathh/Resume Screening/resume_data.json"
    data = ElasticSearch.convert_json_to_pandas(path)
    print("data.columns", data.columns)
    Variables.IndexedFiles = ElasticSearch.index_files(data)
    end = time.time()
    print("Total time took:", end - start)


if __name__ == "__main__":
    app.run(port=5000, use_reloader = True)
