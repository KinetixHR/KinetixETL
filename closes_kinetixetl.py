import logging
logging.basicConfig(filename='closes_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
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

closes_success_flag = True


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
cj_CONTAINERNAME = "dataloaderexports1/closedjobs"

try: 
    container_client=blob_service_client.get_container_client("dataloaderexports1")
    blob_list = container_client.list_blobs(name_starts_with="closedjobs/")
    for blob in blob_list:
        if today_search_string in str(blob):
            if "ClosedReqs" in str(blob):
                logging.info(str(blob))
                logging.info(f"Found {str(blob.name).split('/')[-1]}")
                cj_BLOBNAME = str(blob.name).split("/")[-1]
except Exception as ex:
    logging.warning("No closes found from TR for today")
    closes_success_flag = False
    logging.warning(ex)


if closes_success_flag == True:
    try:
        df_closed = downloadBlobFromAzure(cj_CONTAINERNAME,cj_BLOBNAME)
        df_closed.columns = ["JOB_ID", "ACCOUNT_MANAGER", "REQ_NUMBER", "CLOSED_DATE",
            "CLOSED_REASON", "COMPANY", "CONTACT", "FEE_TIER", "JOB_NAME",
            "JOB_OWNER", "JOB_STAGE_TEXT", "LEVEL", "JOB_OPEN_DATE", "PAY_GRADE",
            "RECORD_TYPE_NAME", "REGIONAL_AREA", "SALARY_HIGH", "SALARY_LOW",
            "JOB_STATUS"]

        for el in df_closed.columns:
            df_closed[el] = df_closed[el].fillna("")
            df_closed["EFFECTIVE_DATE"] = today
        logging.info("Loaded in closes from SFTP...:" + str(df_closed.shape))
    except Exception as ex:
        logging.warning("Issue loading in closes from SFTP File")
        closes_success_flag = False
        logging.warning(ex)


if closes_success_flag == True:
    try:
        cursor = cnxn.cursor()
        for index,row in df_closed.iterrows():
            cursor.execute("""INSERT INTO [dbo].[dw_closes] ("JOB_ID", "ACCOUNT_MANAGER", "REQ_NUMBER", "CLOSED_DATE","CLOSED_REASON", "COMPANY", "CONTACT", "FEE_TIER", "JOB_NAME","JOB_OWNER", "JOB_STAGE_TEXT", "LEVEL", "JOB_OPEN_DATE", "PAY_GRADE","RECORD_TYPE_NAME", "REGIONAL_AREA", "SALARY_HIGH", "SALARY_LOW","JOB_STATUS","EFFECTIVE_DATE") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row.JOB_ID, row.ACCOUNT_MANAGER, row.REQ_NUMBER, row.CLOSED_DATE,row.CLOSED_REASON, row.COMPANY, row.CONTACT, row.FEE_TIER, row.JOB_NAME,row.JOB_OWNER, row.JOB_STAGE_TEXT, row.LEVEL, row.JOB_OPEN_DATE, row.PAY_GRADE,row.RECORD_TYPE_NAME, row.REGIONAL_AREA, row.SALARY_HIGH, row.SALARY_LOW,row.JOB_STATUS,row.EFFECTIVE_DATE)
        cnxn.commit()
        cursor.close()
        logging.info("Done loading closes")
    except Exception as ex:
        logging.warning("Issue loading in closes to SQL Server")
        logging.warning(ex)

if closes_success_flag == True:
    logging.info("Done loading all data into closes table successfully, exiting script.")
else:
    logging.warn("Something went wrong with loading closes, script exiting.")


