from flask import Flask, render_template, request
import pandas as pd
import time
import ElasticSearch,Variables
import UserInterface

app = Flask(__name__)


@app.route("/")
def home():
    return render_template("home.html")

@app.route("/get")
def get_bot_response():
    userText = request.args.get('msg')
    Data_Resumes=main(userText.split())
    html=UserInterface.UserInterface.form_html(Data_Resumes)
    # return Data_Resumes.to_html()
    print("html",html)
    return html
    # return Data_Resumes.to_html()

    # return str(Data_Resumes)

def main(query):
    query_skills=query[0]+""
    query_years_of_exp = float(query[1])
    query_job_type = query[2] + ""
    print("main of app query skills",query_skills,query_years_of_exp,query_job_type)

    res1=ElasticSearch.results(Variables.IndexedFiles,query_skills,query_years_of_exp,query_job_type)

    File_names = []
    Exp=[]
    EngineerType=[]
    SkillsFound=[]
    for item in res1['hits']['hits']:
        if bool(item['_source']) is True:
            try:
                if item['_source']['Filename'] not in File_names:
                    File_names.append(item['_source']['Filename'])
                    Exp.append(item['_source']['YearsOfExperience'])
                    EngineerType.append(item['_source']['engineerType'])
                    SkillsFound.append(item['_source']['SkillsFound'])
                    # print("res1 is ",item['_source']['YearsOfExperience'])
                    # print("res1 is ", item['_source']['Filename'])
            except Exception as e:
                print("in except")

    data=pd.DataFrame()
    data['File Name']=File_names
    data['SkillsFound'] = SkillsFound
    data['Engineer Type'] = EngineerType
    data['Experience']=Exp
    Links=ElasticSearch.get_drive_link(File_names)
    data['DriveLinks'] = Links
    data['DriveLinks']= data['DriveLinks'].apply(lambda x: '<a href="{}">{}</a>'.format(x,x))
    print(data.head())
    return data.head(10)
    # return "manju"

@app.before_first_request
def before_first_request():
    print('########### started Indexing')
    start = time.time()
    path1 = "/home/manjunathh/Resume Screening/resume_data.json"
    data1 = ElasticSearch.convert_json_to_pandas(path1)

    path2 = "/home/manjunathh/Resume Screening/ResumesWithHyperLink1.csv"
    data2=ElasticSearch.read_csv(path2)

    data1['DriveLink']=data2['DriveLink']
    print("data.columns", data1.columns)

    Variables.IndexedFiles = ElasticSearch.index_files(data1[0:10])
    end = time.time()
    print("Total time took:", end - start)


if __name__ == "__main__":
    app.run(port=5000, use_reloader = True)
