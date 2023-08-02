import logging
logging.basicConfig(filename='opens_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
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

opens_success_flag = True



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
oj_CONTAINERNAME = "dataloaderexports1/openjobs"   

try: 
    container_client=blob_service_client.get_container_client("dataloaderexports1")
    blob_list = container_client.list_blobs(name_starts_with="openjobs/")
    for blob in blob_list:
        if today_search_string in str(blob):
            logging.info(f"Found {str(blob.name).split('/')[-1]}")
            oj_BLOBNAME = str(blob.name).split("/")[-1]
except Exception as ex:
    logging.warning("Issue with loading open Jobs from TR")
    logging.warning(ex)
    opens_success_flag = False


if opens_success_flag == True:
    try:
        df_opens = downloadBlobFromAzure(oj_CONTAINERNAME,oj_BLOBNAME)
        #logging.info(df_users.columns)
        df_opens["LOAD_DATE"] = today
        df_opens.columns = ["JOB_ID", "REQ_NUMBER","CLOSED_REASON", "COMPANY", "JOB_CREATED_DATE","JOB_NAME", "JOB_OWNER", "JOB_STAGE_TEXT", "JOB_OPEN_DATE", "JOB_STATUS","TARGET_BILLING_DATE","LOAD_DATE"]

        for el in df_opens.columns:
            df_opens[el] = df_opens[el].fillna("")
            #df_users["LOAD_DATE"] = today
        df_opens["JOB_CREATED_DATE"] = pd.to_datetime(df_opens["JOB_CREATED_DATE"])
        df_opens["JOB_CREATED_DATE"] = df_opens["JOB_CREATED_DATE"].dt.strftime('%Y-%m-%d')
        logging.info("Loaded in opens file from SFTP...:" + str(df_opens.shape))
    except Exception as ex:
        logging.warning("Issue loading in opens from SFTP File")
        logging.warning(ex)
        opens_success_flag = False

if opens_success_flag == True:
    cursor = cnxn.cursor()
    try: 
        for index, row in df_opens.iterrows():
            cursor.execute("""INSERT INTO [dbo].[dw_opens] ("JOB_ID", "REQ_NUMBER","CLOSED_REASON", "COMPANY", "JOB_CREATED_DATE","JOB_NAME", "JOB_OWNER", "JOB_STAGE_TEXT", "JOB_OPEN_DATE", "JOB_STATUS","TARGET_BILLING_DATE","LOAD_DATE") values(?,?,?,?,?,?,?,?,?,?,?,?)""", row.JOB_ID, row.REQ_NUMBER, row.CLOSED_REASON, row.COMPANY, row.JOB_CREATED_DATE, row.JOB_NAME, row.JOB_OWNER, row.JOB_STAGE_TEXT, row.JOB_OPEN_DATE, row.JOB_STATUS, row.TARGET_BILLING_DATE, row.LOAD_DATE)
        cnxn.commit()
        cursor.close()
        logging.info("Done loading opens")
    except Exception as ex:
        logging.warning("Issue loading in opens to SQL Server")
        logging.warning(ex)
        opens_success_flag = False
        cursor.close()

if opens_success_flag == True:
    logging.info("Done loading all data into opens table successfully, exiting script.")
else:
    logging.warn("Something went wrong with loading opens, script exiting.")