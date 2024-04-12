import os
import pandas as pd
import sqlalchemy as sa
from dagster import (
    asset,
    Config,
    MetadataValue,
    AssetExecutionContext,
    ResourceParam
)
from .resources import MyConnectionStringResource, MyDatafilesPathResource

@asset(group_name="FMR_data"
) 
def husmat_by_year(context: AssetExecutionContext,
                   publicdata_conn_string: MyConnectionStringResource,
                   publicdata_datafiles_path: MyDatafilesPathResource
                   ) -> None:
    connection_string = publicdata_conn_string.connection_string
    connection_url = sa.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = sa.create_engine(connection_url)

    sql = """
        select
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

    with engine.begin() as conn:
        df_husmat_by_year = pd.read_sql_query(sa.text(sql), conn)

    df_husmat_by_year.to_csv(os.path.join(publicdata_datafiles_path.datafiles_path, "husmat_by_year.csv"))

    context.add_output_metadata(
        metadata={
            "num_records": len(df_husmat_by_year),
            "preview": MetadataValue.md(df_husmat_by_year.to_markdown())
        }
    )
