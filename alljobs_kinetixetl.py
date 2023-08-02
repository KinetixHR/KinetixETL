import logging
logging.basicConfig(filename='alljobs_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

import pandas as pd
import os, uuid
from io import StringIO
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import date
import pyodbc

def downloadBlobFromAzure(containername_,blobname_):
    blob_client = blob_service_client.get_blob_client(container=containername_, blob=blobname_)
    try:
       stream = blob_client.download_blob().readall()
       s=str(stream,"utf-8")
       data = StringIO(s) 
       df = pd.read_csv(data,low_memory=False)
    except Exception as e:
       logging.warning(e)
    return df

alljobs_success_flag = True



try:
    connect_str = "DefaultEndpointsProtocol=https;AccountName=sftpkinetix;AccountKey=9dHu7wYUIc1fT+Wk7WUUPGVxvJuXj+pVets2eDiGUiUN4XJKO2zpBSp5CurJGzofVHSrZNs+Y1d7+ASt/oAIhA==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
except Exception as ex:
    logging.warning("Exception:")
    logging.warning(ex)

server = "kinetixsql.database.windows.net" 
database = "KinetixSQL" 
username = "awhelan" 
password = "5uj7*ZpE8Y$D"
cnxn = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+ password)


today = date.today()
today_search_string = date.today().strftime('%m_%d_%Y')
aj_CONTAINERNAME = "dataloaderexports1/landing"

try: 
    container_client=blob_service_client.get_container_client("dataloaderexports1")
    blob_list = container_client.list_blobs(name_starts_with="landing/")
    for blob in blob_list:
        if (today_search_string in str(blob)):
            if ("AllJobs" in str(blob)):
                logging.info(f"Found {str(blob.name).split('/')[-1]}")
                aj_BLOBNAME = str(blob.name).split("/")[-1]
            else:
                logging.warning(f"Could not find all jobs file in SFTP! + {blob}")
except Exception as ex:
    logging.warning("Issue with loading in All Jobs from TR")
    logging.warning(ex)
    alljobs_success_flag = False


if alljobs_success_flag == True:
    try:
        df_alljobs = downloadBlobFromAzure(aj_CONTAINERNAME,aj_BLOBNAME)
        #df_alljobs.to_csv("alljobs_raw.csv", index=False)
        df_alljobs.drop('Notes', axis=1, inplace=True)
        df_alljobs.columns = ["JOB_ID","CLIENT_REQ_NUMBER","CLOSED_DATE","CLOSED_REASON","COMPANY_NAME","CREATED_BY_ID","CREATED_DATE","GOALS","JOB_FAMILY","JOB_NAME","JOB_OWNER","JOB_STAGE_TEXT","LAST_ACTIVITY_DATE","LAST_MODIFIED_ID","LAST_MODIFIED_DATE","OPEN_DATE","PIPELINE_JOB","PRIMARY_JOB_REQ","PRIMARY_SECONDARY",'RECORD_TYPE_NAME','RECRUITER_WEEKLY_NOTES','STATUS']
        
        df_alljobs["LAST_ACTIVITY_DATE"] = pd.to_datetime(df_alljobs["LAST_ACTIVITY_DATE"],errors = 'coerce')
        df_alljobs["LAST_MODIFIED_DATE"] = pd.to_datetime(df_alljobs["LAST_MODIFIED_DATE"])
        df_alljobs["OPEN_DATE"] = pd.to_datetime(df_alljobs["OPEN_DATE"])
        df_alljobs["CLOSED_DATE"] = pd.to_datetime(df_alljobs["CLOSED_DATE"],errors = 'coerce')
        df_alljobs["CREATED_DATE"] = pd.to_datetime(df_alljobs["CREATED_DATE"])

        df_alljobs["LAST_ACTIVITY_DATE"] = df_alljobs["LAST_ACTIVITY_DATE"].dt.strftime('%Y-%m-%d')
        df_alljobs["LAST_MODIFIED_DATE"] = df_alljobs["LAST_MODIFIED_DATE"].dt.strftime('%Y-%m-%d')
        df_alljobs["OPEN_DATE"] =  df_alljobs["OPEN_DATE"].dt.strftime('%Y-%m-%d')
        df_alljobs["CLOSED_DATE"] = df_alljobs["CLOSED_DATE"].dt.strftime('%Y-%m-%d')
        df_alljobs["CREATED_DATE"] = df_alljobs["CREATED_DATE"].dt.strftime('%Y-%m-%d')

        df_alljobs = df_alljobs.fillna("")

        logging.info(df_alljobs.head())
        logging.info("Loaded in all jobs file from SFTP...:" + str(df_alljobs.shape))
    except Exception as ex:
        logging.warning("Issue loading in all jobs file from SFTP")
        logging.warning(ex)
        alljobs_success_flag_success_flag = False

if alljobs_success_flag == True:
    cursor = cnxn.cursor()
    try: 
        cursor.execute("DELETE FROM [dbo].[dw_jobs]")
        cnxn.commit()
        for index, row in df_alljobs.iterrows():
            cursor.execute("""INSERT INTO [dbo].[dw_jobs] ("JOB_NAME", "JOB_ID", "JOB_STAGE_TEXT", "CREATED_DATE", "OPEN_DATE", "CLOSED_DATE", "CLOSED_REASON", "JOB_OWNER", "CLIENT_REQ_NUMBER", "STATUS", "RECORD_TYPE_NAME", "PRIMARY_SECONDARY", "JOB_FAMILY", "LAST_MODIFIED_DATE", "LAST_MODIFIED_ID", "GOALS", "CREATED_BY_ID", "RECRUITER_WEEKLY_NOTES", "PIPELINE_JOB", "PRIMARY_JOB_REQ", "LAST_ACTIVITY_DATE", "COMPANY_NAME") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row.JOB_NAME, row.JOB_ID, row.JOB_STAGE_TEXT, row.CREATED_DATE, row.OPEN_DATE, row.CLOSED_DATE, row.CLOSED_REASON, row.JOB_OWNER, row.CLIENT_REQ_NUMBER, row.STATUS, row.RECORD_TYPE_NAME, row.PRIMARY_SECONDARY, row.JOB_FAMILY, row.LAST_MODIFIED_DATE, row.LAST_MODIFIED_ID, row.GOALS, row.CREATED_BY_ID, row.RECRUITER_WEEKLY_NOTES, row.PIPELINE_JOB, row.PRIMARY_JOB_REQ, row.LAST_ACTIVITY_DATE, row.COMPANY_NAME)
        cnxn.commit()
        cursor.close()
        logging.info("Done loading all jobs")
    except Exception as ex:
        logging.warning("Issue loading in all jobs to SQL Server")
        logging.warning(ex)
        cursor.close()
        alljobs_success_flag = False

if alljobs_success_flag == True:
    logging.info("Done loading all data into alljobs table successfully, exiting script.")
else:
    logging.warning("Something went wrong with loading alljobs, script exiting.")


