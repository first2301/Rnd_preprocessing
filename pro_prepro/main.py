from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from lib.prepro import Preprocessing, SparkDataFrame

import pandas as pd
import json


app = FastAPI()

prepro = Preprocessing()
sdf = SparkDataFrame()

@app.get('/')
def main():
    return 'main page'

@app.post("/prepro")
async def read_root(request: Request):
    data = await request.json()
    path = data
    extracted_path = prepro.get_all_file_paths(path)
    spark_df = sdf.get_spark_dataframe(extracted_path)
    pandas_df = spark_df.toPandas()
    dump_data = json.dumps(pandas_df)
    return JSONResponse(dump_data)


