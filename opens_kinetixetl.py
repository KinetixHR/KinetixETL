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
from simple_salesforce import Salesforce
import requests


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
    '''
    container_client=blob_service_client.get_container_client("dataloaderexports1")
    blob_list = container_client.list_blobs(name_starts_with="openjobs/")
    for blob in blob_list:
        if today_search_string in str(blob):
            logging.info(f"Found {str(blob.name).split('/')[-1]}")
            oj_BLOBNAME = str(blob.name).split("/")[-1]
    '''
    
    soql_statement = "SELECT Id, Client_Req_Number__c, TR1__Closed_Reason__c, TR1__Account_Name__c, CreatedDate, Name, TR1__Job_Owner__c, Job_Stage_Text__c, TR1__Open_Date__c, TR1__Status__c, Targeted_Billing_Date__c FROM TR1__Job__c WHERE TR1__Status__c != 'Closed' AND TR1__Status__c != 'On Hold' AND (NOT Name LIKE '%funnel%') AND (NOT TR1__Account_Name__c LIKE '%test%') AND Record_Type_Name__c LIKE '%RPO%' AND TR1__Status__c != 'Hold' AND (NOT TR1__Account_Name__c LIKE '%Kinetix%') AND (NOT TR1__Account_Name__c LIKE '%training%')"


    session = requests.Session()
    # Setting up salesforce functionality
    sf = Salesforce(password='Kinetix3', username='awhelan@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session) 

    #generator on the results page
    fetch_results = sf.bulk.TR1__Job__c.query_all(soql_statement, lazy_operation=True)

    all_results = []
    for list_results in fetch_results:
        all_results.extend(list_results)
    df_opens = pd.DataFrame(all_results)
    df_opens = df_opens.drop(columns=['attributes'])
    logging.info(df_opens.shape)
    df_opens.columns = ["JOB_ID", "REQ_NUMBER","CLOSED_REASON", "COMPANY", "JOB_CREATED_DATE","JOB_NAME", "JOB_OWNER", "JOB_STAGE_TEXT", "JOB_OPEN_DATE", "JOB_STATUS","TARGET_BILLING_DATE"]

    logging.info(df_opens.columns)
    logging.info(df_opens.head(2))
    logging.info(f"Successfully loaded jobs from API: {df_opens.shape}")



except Exception as ex:
    logging.warning("Issue with loading open Jobs from API")
    logging.warning(ex)
    opens_success_flag = False


if opens_success_flag == True:
    try:
        #df_opens = downloadBlobFromAzure(oj_CONTAINERNAME,oj_BLOBNAME)
        #logging.info(df_users.columns)
        df_opens["EFFECTIVE_DATE"] = today
        #df_opens = df_opens.drop('Unnamed: 0',axis = 1)
        #logging.info(df_opens.columns)
        logging.info("want these:",["JOB_ID", "REQ_NUMBER","CLOSED_REASON", "COMPANY", "JOB_CREATED_DATE","JOB_NAME", "JOB_OWNER", "JOB_STAGE_TEXT", "JOB_OPEN_DATE", "JOB_STATUS","TARGET_BILLING_DATE","EFFECTIVE_DATE"]
)
        df_opens.columns = ["JOB_ID", "REQ_NUMBER","CLOSED_REASON", "COMPANY", "JOB_CREATED_DATE","JOB_NAME", "JOB_OWNER", "JOB_STAGE_TEXT", "JOB_OPEN_DATE", "JOB_STATUS","TARGET_BILLING_DATE","EFFECTIVE_DATE"]

        for el in df_opens.columns:
            df_opens[el] = df_opens[el].fillna("")
            #df_users["LOAD_DATE"] = today
        df_opens["JOB_CREATED_DATE"] = pd.to_datetime(df_opens["JOB_CREATED_DATE"])
        df_opens["JOB_CREATED_DATE"] = df_opens["JOB_CREATED_DATE"].dt.strftime('%Y-%m-%d')
        logging.info("Loaded in opens file & transformed data from API...:" + str(df_opens.shape))
    except Exception as ex:
        logging.warning("Issue loading/transforming in opens from API")
        logging.warning(ex)
        opens_success_flag = False

if opens_success_flag == True:
    cursor = cnxn.cursor()
    try: 
        for index, row in df_opens.iterrows():
            cursor.execute("""INSERT INTO [dbo].[dw_opens] ("JOB_ID", "REQ_NUMBER","CLOSED_REASON", "COMPANY", "JOB_CREATED_DATE","JOB_NAME", "JOB_OWNER", "JOB_STAGE_TEXT", "JOB_OPEN_DATE", "JOB_STATUS","TARGET_BILLING_DATE","EFFECTIVE_DATE") values(?,?,?,?,?,?,?,?,?,?,?,?)""", row.JOB_ID, row.REQ_NUMBER, row.CLOSED_REASON, row.COMPANY, row.JOB_CREATED_DATE, row.JOB_NAME, row.JOB_OWNER, row.JOB_STAGE_TEXT, row.JOB_OPEN_DATE, row.JOB_STATUS, row.TARGET_BILLING_DATE, row.EFFECTIVE_DATE)
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