from snowflake.snowpark import Session

import streamlit as st

"""
* get_snowpark_session : 스노우플레이크 접속 함수

"""
def get_snowpark_session() -> Session:
    connection_parameters = {
       "ACCOUNT":"dl52763.ap-northeast-2.aws",
        "USER":"penta",
        "PASSWORD":"PEnta2848!",
        "ROLE":"ACCOUNTADMIN",
        "DATABASE":"SNOWFLAKE",
        "SCHEMA":"ML",
        "WAREHOUSE":"COMPUTE_WH"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()

def main():
    session = get_snowpark_session()

    context_df = session.sql("select current_role(), current_database(), current_schema(), current_warehouse()")
    context_df.show(2)

