import polars as pl
import pandas as pd
import duckdb
from functools import wraps
from function_time import timeit

@timeit
def transform_data(df: pl.LazyFrame) -> pl.DataFrame:
    """
    Transforms O'Reilly data in the form of a dictionary into the final datatset. 
    The final output will include the person's first and last name, email, content_last_accessed date sorted
    by ascending (Last first), and if their account is active

    Args:
        data: Oreilly Data in a polars LazyFrame

    Returns:
        Polars LazyFrame
    """


    df = df.filter(pl.col("active")=="true")
    df = df.drop_nulls(subset=["content_last_accessed"])
    df = df.sort("content_last_accessed", descending=False)
    
    return(df.select(["first_name", "last_name", "email", "content_last_accessed", "active"])[:10].collect())

@timeit
def transform_data_pandas(df: dict) -> pd.DataFrame:
    """
    Transforms O'Reilly data in the form of a dictionary into the final datatset. 
    The final output will include the person's first and last name, email, content_last_accessed date sorted
    by ascending (Last first), and if their account is active

    Args:

        data: Oreilly Data in a Dictonary

    Returns:
        Pandas Dataframe
    """

    #df = pd.DataFrame(data)

    df = df[df["active"]==True]
    df = df.dropna(subset=["content_last_accessed"])
    df = df.sort_values(by="content_last_accessed", ascending=True)
    
    return(df[["first_name", "last_name", "email", "content_last_accessed", "active"]][:10])

@timeit
def transform_data_duckdb(df: dict) -> pd.DataFrame:
    """
    Transforms O'Reilly data in the form of a dictionary into the final datatset. 
    The final output will include the person's first and last name, email, content_last_accessed date sorted
    by ascending (Last first), and if their account is active

    Args:

        data: Oreilly Data in a Dictonary

    Returns:
        Pandas Dataframe
    """

    #df = pd.DataFrame(data)

    results = duckdb.sql("""
                SELECT first_name, last_name, email, content_last_accessed, active
               FROM df
               WHERE active = 'true'
               ORDER BY content_last_accessed ASC
               LIMIT 10;
                """)
    
    return(results.show())


    