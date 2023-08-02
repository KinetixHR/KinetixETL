import logging
logging.basicConfig(filename='example.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
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
closes_success_flag = True
placements_success_flag = True
alljobs_success_flag = True
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
oj_CONTAINERNAME = "dataloaderexports1/openjobs"   
cj_CONTAINERNAME = "dataloaderexports1/closedjobs"
pj_CONTAINERNAME = "dataloaderexports1/closedjobs"
aj_CONTAINERNAME = "dataloaderexports1/landing"
uj_CONTAINERNAME = "dataloaderexports1/landing"
cands_CONTAINERNAME = ""
cands_BLOBNAME = ""

# Search for today's file in Azure containers 

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
    
try: 
    container_client=blob_service_client.get_container_client("dataloaderexports1")
    blob_list = container_client.list_blobs(name_starts_with="openjobs/")
    for blob in blob_list:
        if today_search_string in str(blob):
            logging.info(f"Found {str(blob.name).split('/')[-1]}")
            oj_BLOBNAME = str(blob.name).split("/")[-1]
except:
    logging.warning("Issue with loading open Jobs from TR")
    opens_success_flag = False

try: 
    blob_list = container_client.list_blobs(name_starts_with="closedjobs/")
    for blob in blob_list:
        if today_search_string in str(blob):
            if "ClosedReqs" in str(blob):
                logging.info(f"Found {str(blob.name).split('/')[-1]}")
                cj_BLOBNAME = str(blob.name).split("/")[-1]
            if "Placement" in str(blob):
                logging.info(f"Found {str(blob.name).split('/')[-1]}")
                pj_BLOBNAME = str(blob.name).split("/")[-1]
except:
    logging.warning("No closed and/or placement Jobs found for today")
    closes_success_flag = False
    placements_success_flag = False




if alljobs_success_flag == True:
    try:
        df_alljobs = downloadBlobFromAzure(aj_CONTAINERNAME,aj_BLOBNAME)
        df_alljobs.columns = ['JOB_NAME', 'JOB_ID', 'JOB_STAGE_TEXT', 'CREATED_DATE', 'JOB_OPEN_DATE', 'JOB_CLOSED_DATE', 'CLOSED_REASON', 'JOB_OWNER', 'CLIENT_REQ_NO', 'JOB_STATUS', 'JOB_RECORD_TYPE', 'PRIMARY_SECONDARY', 'ASSOCIATED_PRIMARY_JOB', 'JOB_FAMILY', 'JOB_LAST_MODIFIED_DATE', 'JOB_LAST_MODIFIED_BY', 'JOB_GOALS', 'JOB_CREATED_BY', 'JOB_NOTES', 'PIPELINE_JOB', 'PRIMARY_JOB_REQ', 'JOB_LAST_ACTIVITY_DATE', 'COMPANY_NAME']

        for el in df_alljobs.columns:
            df_alljobs[el] = df_alljobs[el].fillna("")
            #df_alljobs["LOAD_DATE"] = today
        logging.info("Loaded in all jobs file from SFTP...:" + str(df_alljobs.shape))
    except Exception as ex:
        logging.warning("Issue loading in all jobs file from SFTP")
        logging.warning(ex)
        alljobs_success_flag_success_flag = False


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

if opens_success_flag == True:
    try:
        df_users = downloadBlobFromAzure(oj_CONTAINERNAME,oj_BLOBNAME)
        df_users.columns = ["JOB_ID", "REQ_NUMBER","CLOSED_REASON", "COMPANY", "JOB_CREATED_DATE","JOB_NAME", "JOB_OWNER", "JOB_STAGE_TEXT", "JOB_OPEN_DATE", "JOB_STATUS","TARGET_BILLING_DATE","LOAD_DATE"]

        for el in df_opens.columns:
            df_users[el] = df_users[el].fillna("")
            df_users["LOAD_DATE"] = today
        logging.info("Loaded in opens file from SFTP...:" + str(df_users.shape))
    except:
        logging.warning("Issue loading in opens from SFTP File")
        opens_success_flag = False

if closes_success_flag == True:
    try:
        df_closed  = downloadBlobFromAzure(cj_CONTAINERNAME,cj_BLOBNAME)
        df_closed.columns = ["JOB_ID", "ACCOUNT_MANAGER", "REQ_NUMBER", "CLOSED_DATE",
            "CLOSED_REASON", "COMPANY", "CONTACT", "FEE_TIER", "JOB_NAME",
            "JOB_OWNER", "JOB_STAGE_TEXT", "LEVEL", "JOB_OPEN_DATE", "PAY_GRADE",
            "RECORD_TYPE_NAME", "REGIONAL_AREA", "SALARY_HIGH", "SALARY_LOW",
            "JOB_STATUS"]

        for el in df_closed.columns:
            df_closed[el] = df_closed[el].fillna("")
            df_closed["LOAD_DATE"] = today
        logging.info("Loaded in closes from SFTP...:" + str(df_closed.shape))
    except:
        logging.warning("Issue loading in closes from SFTP File")
        closes_success_flag = False

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


cursor = cnxn.cursor()
#Insert Dataframe into SQL Server:

if alljobs_success_flag == True:
    try: 
        for index, row in df_alljobs.iterrows():
            cursor.execute("""INSERT INTO [dbo].[dw_jobs] ("JOB_NAME", "JOB_ID", "JOB_STAGE_TEXT", "CREATED_DATE", "JOB_OPEN_DATE", "JOB_CLOSED_DATE", "CLOSED_REASON", "JOB_OWNER", "CLIENT_REQ_NO", "JOB_STATUS", "JOB_RECORD_TYPE", "PRIMARY_SECONDARY", "ASSOCIATED_PRIMARY_JOB", "JOB_FAMILY", "JOB_LAST_MODIFIED_DATE", "JOB_LAST_MODIFIED_BY", "JOB_GOALS", "JOB_CREATED_BY", "JOB_NOTES", "PIPELINE_JOB", "PRIMARY_JOB_REQ", "JOB_LAST_ACTIVITY_DATE", "COMPANY_NAME") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row.JOB_NAME, row.JOB_ID, row.JOB_STAGE_TEXT, row.CREATED_DATE, row.JOB_OPEN_DATE, row.JOB_CLOSED_DATE, row.CLOSED_REASON, row.JOB_OWNER, row.CLIENT_REQ_NO, row.JOB_STATUS, row.JOB_RECORD_TYPE, row.PRIMARY_SECONDARY, row.ASSOCIATED_PRIMARY_JOB, row.JOB_FAMILY, row.JOB_LAST_MODIFIED_DATE, row.JOB_LAST_MODIFIED_BY, row.JOB_GOALS, row.JOB_CREATED_BY, row.JOB_NOTES, row.PIPELINE_JOB, row.PRIMARY_JOB_REQ, row.JOB_LAST_ACTIVITY_DATE, row.COMPANY_NAME)
        cnxn.commit()
        cursor.close()
        logging.info("Done loading all jobs")
    except Exception as ex:
        logging.warning("Issue loading in all jobs to SQL Server")
        logging.warning(ex)


if user_success_flag == True:
    try: 
        for index, row in df_users.iterrows():
            cursor.execute("""INSERT INTO [dbo].[dw_recruiter] ("USER_ID", "ACTIVE_FLAG", "COMPANY_NAME", "CREATED_DATE", "USER_FULL_NAME", "LAST_MODIFIED_DATE", "PAYROLL_NAME", "USER_ROLE", "MANAGER_FULL_NAME") values(?,?,?,?,?,?,?,?,?)""", row.USER_ID, row.ACTIVE_FLAG, row.COMPANY_NAME, row.CREATED_DATE, row.USER_FULL_NAME, row.LAST_MODIFIED_DATE, row.PAYROLL_NAME, row.USER_ROLE, row.MANAGER_FULL_NAME)
        cnxn.commit()
        cursor.close()
        logging.info("Done loading all jobs")
    except Exception as ex:
        logging.warning("Issue loading in users to SQL Server")
        logging.warning(ex)



if opens_success_flag == True:
    try: 
        for index, row in df_opens.iterrows():
            cursor.execute("""INSERT INTO [dbo].[dw_opens] ("JOB_ID", "REQ_NUMBER","CLOSED_REASON", "COMPANY", "JOB_CREATED_DATE","JOB_NAME", "JOB_OWNER", "JOB_STAGE_TEXT", "JOB_OPEN_DATE", "JOB_STATUS","TARGET_BILLING_DATE","LOAD_DATE") values(?,?,?,?,?,?,?,?,?,?,?,?)""", row.JOB_ID, row.REQ_NUMBER, row.CLOSED_REASON, row.COMPANY, row.JOB_CREATED_DATE, row.JOB_NAME, row.JOB_OWNER, row.JOB_STAGE_TEXT, row.JOB_OPEN_DATE, row.JOB_STATUS, row.TARGET_BILLING_DATE, row.LOAD_DATE)
        cnxn.commit()
        cursor.close()
        logging.info("Done loading opens")
    except Exception as ex:
        logging.warning("Issue loading in opens to SQL Server")
        logging.warning(ex)

if closes_success_flag == True:
    try:
        cursor = cnxn.cursor()
        for index,row in df_closed.iterrows():
            cursor.execute("""INSERT INTO [dbo].[dw_closes] ("JOB_ID", "ACCOUNT_MANAGER", "REQ_NUMBER", "CLOSED_DATE","CLOSED_REASON", "COMPANY", "CONTACT", "FEE_TIER", "JOB_NAME","JOB_OWNER", "JOB_STAGE_TEXT", "LEVEL", "JOB_OPEN_DATE", "PAY_GRADE","RECORD_TYPE_NAME", "REGIONAL_AREA", "SALARY_HIGH", "SALARY_LOW","JOB_STATUS","LOAD_DATE") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row.JOB_ID, row.ACCOUNT_MANAGER, row.REQ_NUMBER, row.CLOSED_DATE,row.CLOSED_REASON, row.COMPANY, row.CONTACT, row.FEE_TIER, row.JOB_NAME,row.JOB_OWNER, row.JOB_STAGE_TEXT, row.LEVEL, row.JOB_OPEN_DATE, row.PAY_GRADE,row.RECORD_TYPE_NAME, row.REGIONAL_AREA, row.SALARY_HIGH, row.SALARY_LOW,row.JOB_STATUS,row.LOAD_DATE)
        cnxn.commit()
        cursor.close()
        logging.info("Done loading closes")
    except Exception as ex:
        logging.warning("Issue loading in closes to SQL Server")
        logging.warning(ex)


if placements_success_flag == True:
    try:
        cursor = cnxn.cursor()
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

logging.info(f"Opens Status: {opens_success_flag}")
logging.info(f"Closes Status: {closes_success_flag}")
logging.info(f"Placements Status: {placements_success_flag}")
logging.info(f"All Jobs Status: {alljobs_success_flag}")
logging.info(f"Users Status: {user_success_flag}")

logging.info("Done loading all data into tables, exiting script.")