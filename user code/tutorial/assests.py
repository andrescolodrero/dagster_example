import json
import pyodbc
import time
import subprocess
import pandas as pd
import logging
import os
import re
import requests
from datetime import datetime

from dagster import AssetExecutionContext, MetadataValue, asset

# asset dependencies can be inferred from parameter names
@asset()
def hackernews_top_stories(context: AssetExecutionContext):
    context.log.setLevel("DEBUG")

    driver = '{ODBC Driver 18 for SQL Server}'
    server = "mysqlserver"
    database = "PublicData"
    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes"
    context.log.info(f"Connection String: {connection_string}")
    context.log.info(datetime.now())
    sql = """
        select top 10
                byggingarar,
                sum(husmat) husmat
            from FMR.vFMREignir
            where ByggingarStigId = '7'
                and isnull(byggingarar,0) > 1000
            group by
                byggingarar
            order by
                byggingarar
    """

    try:
        cnxn = pyodbc.connect(connection_string)
        context.log.info(cnxn)
        cursor = cnxn.cursor()
        context.log.info('Connection successful')  # Log success message
        cursor.execute(sql)
        row = cursor.fetchone()
        while row:
            context.log.info(row[0])
            row = cursor.fetchone()
    except pyodbc.Error as ex:
        context.log.error("pyodbc.Error occurred:")
        context.log.error(f"Error message: {ex}")
        # Print the entire exception traceback for additional details
        import traceback
        traceback.print_exc()
        raise
    except Exception as e:
        context.log.error("An unexpected error occurred:")
        context.log.error(f"Error message: {e}")
        # Print the entire exception traceback for additional details
        import traceback
        traceback.print_exc()
        raise
