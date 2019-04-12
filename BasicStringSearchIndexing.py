from elasticsearch import Elasticsearch
import os
import glob
import PyPDF2
import logging
import pandas as pd
import warnings
import time
from multiprocessing import Pool

warnings.filterwarnings("ignore")


def set_os_path(path):
    os.chdir(path)
    files=glob.glob("*.*")
    logging.info("No of files read",len(files))
    print("No of files read",len(files))
    return files

def extract_pdf_files(files):
    this_loc=1
    df=pd.DataFrame(columns=("name","content"))
    for file in files:
        pdfFileObj=open(file,'rb')
        pdfReader =PyPDF2.PdfFileReader(pdfFileObj)
        n_pages=pdfReader.numPages
        this_doc =""
        for i in range(n_pages):
            pageObj=pdfReader.getPage(i)
            this_text=pageObj.extractText()
            this_doc+=this_text
            # file_path = os.getcwd() + "/" + file
            # file_path="<a href =file:"+ file_path+">Hi</a>"
            # print(file_path)
        df.loc[this_loc]=file,this_doc
        this_loc=this_loc+1
    # print(df.shape)
    return df

def index_files(df):
    es=Elasticsearch()
    col_names=df.columns
    # print("length of df",df.shape)
    for row_number in range(df.shape[0]):
        body=dict([(name,str(df.iloc[row_number][name])) for name in col_names])
        print("body",body)
        es.index(index='data_science',doc_type='books',body=body)
    return es

def results(indexedfiles,query):
    search_results=indexedfiles.search(index='data_science',doc_type='books',
                                       body={"_source":"name",
                                             'query':{
                                                 "match_phrase":{"content":query}
                                             }
                                             })
    return search_results
if __name__ == '__main__':
    pool = Pool(3)  # number of cores you want to use
    start = time.time()
    files=set_os_path("/home/manjunathh/Resume Screening/Employee Resumes/Eximius Employees Resumes PDF Format/")
    # data = pool.map(extract_pdf_files, files)  # creates a list of the loaded df's
    data=extract_pdf_files(files)

    print(data.head(10))
    IndexedFiles=index_files(data)
    end = time.time()
    print("Total time took:",end - start)
    while True:
        print("Enter the search squery:")
        query=input()+""
        res1=results(IndexedFiles,query)
        print("res['hits']",res1['hits'])
        # print(data[data['content'].str.contains(query)])
        # print("shape is :",data[data['content'].str.contains(query)].shape)
