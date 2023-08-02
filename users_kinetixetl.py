import logging
logging.basicConfig(filename='users_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
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

user_success_flag = True


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

uj_CONTAINERNAME = "dataloaderexports1/landing"


try: 
    container_client=blob_service_client.get_container_client("dataloaderexports1")
    blob_list = container_client.list_blobs(name_starts_with="landing/")
    for blob in blob_list:
        if (today_search_string in str(blob)) and ("User" in str(blob)):
            logging.info(f"Found {str(blob.name).split('/')[-1]}")
            uj_BLOBNAME = str(blob.name).split("/")[-1]
except:
    logging.warning("Issue with loading in Users from TR")
    user_success_flag = False

if user_success_flag == True:
    try:
        df_users = downloadBlobFromAzure(uj_CONTAINERNAME,uj_BLOBNAME)
        df_users.columns = ['USER_ID', 'ACTIVE_FLAG', 'COMPANY_NAME', 'CREATED_DATE', 'USER_FULL_NAME', 'LAST_MODIFIED_DATE', 'PAYROLL_NAME', 'USER_ROLE', 'MANAGER_FULL_NAME']

        for el in df_users.columns:
            df_users[el] = df_users[el].fillna("")
            #df_users["LOAD_DATE"] = today
        logging.info("Loaded in user file from SFTP...:" + str(df_users.shape))
    except:
        logging.warning("Issue loading in users from SFTP File")
        user_success_flag = False

if user_success_flag == True:
    cursor = cnxn.cursor()
    try:
        cursor.execute("DELETE FROM [dbo].[dw_recruiters]")
        cnxn.commit()
        for index, row in df_users.iterrows():
            cursor.execute("""INSERT INTO [dbo].[dw_recruiters] ("USER_ID", "ACTIVE_FLAG", "COMPANY_NAME", "CREATED_DATE", "USER_FULL_NAME", "LAST_MODIFIED_DATE", "PAYROLL_NAME", "USER_ROLE", "MANAGER_FULL_NAME") values(?,?,?,?,?,?,?,?,?)""", row.USER_ID, row.ACTIVE_FLAG, row.COMPANY_NAME, row.CREATED_DATE, row.USER_FULL_NAME, row.LAST_MODIFIED_DATE, row.PAYROLL_NAME, row.USER_ROLE, row.MANAGER_FULL_NAME)
        cnxn.commit()
        cursor.close()
        logging.info("Done loading all recruiters")
    except Exception as ex:
        logging.warning("Issue loading in users to SQL Server")
        logging.warning(ex)
        cursor = cnxn.cursor()


if user_success_flag == True:
    logging.info("Done loading all data into recruiter table successfully, exiting script.")
else:
    logging.warn("Something went wrong with loading recruiters, script exiting.")


