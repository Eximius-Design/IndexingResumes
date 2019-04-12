# Ref:https://towardsdatascience.com/elasticsearch-tutorial-for-beginners-using-python-b9cb48edcedc

from elasticsearch import Elasticsearch
import os
import glob
import PyPDF2
import logging
import pandas as pd
import warnings
import time

warnings.filterwarnings("ignore")

def convert_json_to_pandas(path):
    data=pd.read_json(path)
    # data.to_csv('/home/manjunathh/Resume Screening/Resume_data.csv')
    return data


def index_files(df):
    es=Elasticsearch()
    col_names=df.columns
    # print("length of df",df.shape)
    i=0
    for row_number in range(df.shape[0]):
        body=dict([(name,str(df.iloc[row_number][name])) for name in col_names])
        print("body",i,body)
        i+=1
        es.index(index='data_science',doc_type='books',body=body)
    return es


def results(indexedfiles,query_skills,query_exp,query_jobtype):
    print("query exp is :",type(query_exp))
    search_results_skills=indexedfiles.search(index='data_science',doc_type='books',body={
                                                                    "_source": "Filename",
                                                                    "from": 0, "size": 5000,
                                                                    'query':{
                                                                        'bool':{
                                                                            'must':[{
                                                                                    'match':{
                                                                                        'SkillsFound':query_skills
                                                                                    }
                                                                                },
                                                                                {
                                                                                    'match': {
                                                                                        'engineerType':query_jobtype
                                                                                    }
                                                                                },

                                                                            ]
                                                                            ,
                                                                            "filter": {
                                                                                "range": {
                                                                                    "YearsOfExperience": {
                                                                                        "gte": query_exp
                                                                                    }
                                                                                }
                                                                            }

                                                                            # "filter": {
                                                                            #     'match': {
                                                                            #         'YearsOfExperience': query_exp
                                                                            #     }
                                                                            # }
                                                                        }
                                                                    }
                                                                })


    return search_results_skills

if __name__ == '__main__':
    start = time.time()
    path = "/home/manjunathh/Resume Screening/resume_data.json"
    data = convert_json_to_pandas(path)
    print("data.columns",data.columns)
    IndexedFiles=index_files(data)
    end = time.time()
    print("Total time took:",end - start)
    while True:
        print("Enter the search query for skills:")
        query_skills=input()+""

        print("Enter the search query for Experiance:")
        query_years_of_exp = float(input())

        print("Enter the search query for Job Profile type:")
        query_job_type = input() + ""

        res1=results(IndexedFiles,query_skills,query_years_of_exp,query_job_type)
        Resumes=[]
        print(res1['hits']['hits'])
        for item in res1['hits']['hits']:
            if bool(item['_source']) is True:
                if item['_source']['Filename'] not in Resumes:
                    Resumes.append(item['_source']['Filename'])
            # print(item['_source']['Filename'])
        # if len(Resumes)==0:

        print("Resumes",Resumes)
        print("Total resumes",len(Resumes))