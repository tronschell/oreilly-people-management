from get_data_api import get_data_api
from transform import transform_data, transform_data_pandas, transform_data_duckdb
import json
import pandas as pd
import polars as pl

def main() -> None:
    
    data = get_data_api()
    data = json.loads(data)
    data = data["results"]
    
    data = pd.DataFrame(data)
    pldata = pl.LazyFrame(data)

    print(transform_data_pandas(data))
    #print(transform_data(pldata))
    #print(transform_data_duckdb(data))
   

if __name__ == "__main__":
    main()
