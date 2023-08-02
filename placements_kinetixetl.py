import logging
logging.basicConfig(filename='placements_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
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

placements_success_flag = True

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
pj_CONTAINERNAME = "dataloaderexports1/closedjobs"

try: 
    container_client=blob_service_client.get_container_client("dataloaderexports1")
    blob_list = container_client.list_blobs(name_starts_with="closedjobs/")
    for blob in blob_list:
        if today_search_string in str(blob):
            if "Placement" in str(blob):
                logging.info(f"Found {str(blob.name).split('/')[-1]}")
                pj_BLOBNAME = str(blob.name).split("/")[-1]
except:
    logging.warning("No placement Jobs found in TR for today")
    placements_success_flag = False

if placements_success_flag == True:
    try:
        df_placements  = downloadBlobFromAzure(pj_CONTAINERNAME,pj_BLOBNAME)
        df_placements.columns = ["ACTUAL_INVOICE_AMOUNT", "APPROVED_FOR_BILLING",
            "BILLING_DATE", "BILLING_NOTES", "BILLING_TERM", "COMMISSIONABLE_DATE",
            "FALLOFF", "FALLOFF_REASON", "GROSS_INVOICE_AMOUNT",
            "INTERNAL_EXTERNAL", "PERSON_PLACED", "PLACEMENT", "START_DATE",
            "PLACEMENT_RECORD_TYPE_NAME", "JOB_ID"]

        for el in df_placements.columns:
            df_placements[el] = df_placements[el].fillna("")
            df_placements["LOAD_DATE"] = today
        logging.info("Loaded in placements from SFTP...:" + str(df_placements.shape))
    except:
        logging.warning("Issue loading in placements from SFTP File")
        placements_success_flag = False


if placements_success_flag == True:
    try:
        cursor = cnxn.cursor()
        cursor.execute("DELETE FROM [dbo].[dw_placements]")
        cnxn.commit()
        for index,row in df_placements.iterrows():
            cursor.execute("""INSERT INTO [dbo].[dw_placements] ("ACTUAL_INVOICE_AMOUNT", "APPROVED_FOR_BILLING",
            "BILLING_DATE", "BILLING_NOTES", "BILLING_TERM", "COMMISSIONABLE_DATE",
            "FALLOFF", "FALLOFF_REASON", "GROSS_INVOICE_AMOUNT",
            "INTERNAL_EXTERNAL", "PERSON_PLACED", "PLACEMENT", "START_DATE",
            "PLACEMENT_RECORD_TYPE_NAME", "JOB_ID","LOAD_DATE") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row.ACTUAL_INVOICE_AMOUNT, row.APPROVED_FOR_BILLING,
            row.BILLING_DATE, row.BILLING_NOTES, row.BILLING_TERM, row.COMMISSIONABLE_DATE,
            row.FALLOFF, row.FALLOFF_REASON, row.GROSS_INVOICE_AMOUNT,
            row.INTERNAL_EXTERNAL, row.PERSON_PLACED, row.PLACEMENT, row.START_DATE,
            row.PLACEMENT_RECORD_TYPE_NAME, row.JOB_ID,row.LOAD_DATE)
        cnxn.commit()
        cursor.close()
        logging.info("Done loading placements")
    except Exception as ex:
        logging.warning("Issue loading in placements to SQL Server")
        logging.warning(ex)


if placements_success_flag == True:
    logging.info("Done loading all data into placements table successfully, exiting script.")
else:
    logging.warn("Something went wrong with loading placements, script exiting.")

