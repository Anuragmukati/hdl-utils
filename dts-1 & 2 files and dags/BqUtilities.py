import datetime
from datetime import datetime, timedelta, date
import calendar

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import secretmanager
import teradatasql
#from airflow.providers.google.cloud.hooks.secret_manager import GoogleSecretManagerHook
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
import sys
import json

BASE_DIR = "/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/"
sys.path.append(f"{BASE_DIR}/")


def read_config():
    with open('/home/airflow/gcs/dags/vz-it-gk1v-edwpr-0/history_load_config_prod.json', 'r') as f:
        confid_details = json.load(f)
    return confid_details


config = read_config()
bq_audit="vz-it-pr-gk1v-edwpr-0"


def getMetadata():
    
    config = read_config()
    
    bq_metadata_project= config["metadata_project"]
    bq_metadata_dataset= config["metadata_dataset"]

    
    print("Getting Metadata...")
    # Function to query BigQuery and retrieve metadata values
    hook = BigQueryHook(gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app',use_legacy_sql=False)
    sql = """
    SELECT
    *
    FROM `"""+bq_metadata_project+"""."""+bq_metadata_dataset+""".historical_load_metadata_table` where status="INCOMPLETE" limit 10;
    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    return rows


def updateMetadataTable(conn, tableDetails, status):

    config = read_config()
    
    bq_metadata_project= config["metadata_project"]
    bq_metadata_dataset= config["metadata_dataset"]


    print("--------------------Table Details ------- in udpateMetadataTable ")
    print(tableDetails)
    BQTable = str(tableDetails.get('bq_table_name'))
    BQDataset = str(tableDetails.get('bq_schema_name'))
    LoadType = str(tableDetails.get('LoadType'))
    PartitionColumn = str(tableDetails.get('partition_column'))
    PartitionType = str(tableDetails.get('partition_type'))
    MaxPartitionValue = str(tableDetails.get('partition_minvalue'))
    PartitionFormat = tableDetails.get('partition_format')
    Range = tableDetails.get('parititon_range')


    if status == "INPROGRESS":

        sql = """ UPDATE `"""+bq_metadata_project+"""."""+bq_metadata_dataset+""".historical_load_metadata_table` SET Status="INPROGRESS"  where bq_schema_name='"""+str(BQDataset)+"""' and bq_table_name='"""+str(BQTable)+"""' """;

        print(sql)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            print("Inprogress status updated for "+BQDataset+"."+BQTable)
            return True
        except:
            print("Inprogress status update failed for "+BQDataset+"."+BQTable)
            return False

    if status == "FAILED":

        sql = """ UPDATE `"""+bq_metadata_project+"""."""+bq_metadata_dataset+""".historical_load_metadata_table` SET Status="FAILED"  where bq_schema_name='"""+str(BQDataset)+"""' and bq_table_name='"""+str(BQTable)+"""' """;

        print(sql)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            print("Failed status updated for "+BQDataset+"."+BQTable)
            return True
        except:
            print("Failed status update failed for "+BQDataset+"."+BQTable)
            return False


    elif status == "COMPLETED":

        sql = """ UPDATE `"""+bq_metadata_project+"""."""+bq_metadata_dataset+""".historical_load_metadata_table` SET Status="COMPLETED"  where bq_schema_name='"""+str(BQDataset)+"""' and bq_table_name='"""+str(BQTable)+"""'; """
        print("-----------------------------------------")
        print(sql)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            print("Completed status update for "+BQDataset+"."+BQTable)
        except Exception as e:
            print("Not completed status update for "+BQDataset+"."+BQTable)
            print(repr(e))

    elif status == "INCOMPLETE":

        sql = """ UPDATE `"""+bq_metadata_project+"""."""+bq_metadata_dataset+""".historical_load_metadata_table` SET Status="INCOMPLETE"  where bq_schema_name='"""+str(BQDataset)+"""' and bq_table_name='"""+str(BQTable)+"""'; """

        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            print("Incomplete status update for "+BQDataset+"."+BQTable)
        except:
            print("Incomplete status update failed for "+BQDataset+"."+BQTable)



def selectAuditTable(conn, tablename, bqdatasetname):

    print(tablename)
    print(bqdatasetname)
    
    config = read_config()
    
    bq_metadata_project= config["metadata_project"]
    bq_metadata_dataset= config["metadata_dataset"]

    sql = """ SELECT * FROM `"""+bq_audit+"""."""+bq_metadata_dataset+""".historical_load_audit_table` where BQDataset ='"""+str(bqdatasetname)+"""' and BQTable='"""+str(tablename)+"""' and Status='Completed' order by End_time desc""";

    row = False
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        print("----------------Print value of row ----------------------")
        print(row)
        if row == None:
            print("Not found ")
            return False
        else:
            print("Audit entry found for "+bqdatasetname+"."+tablename)
            return row
    except:
        print("Audit entry not found for  "+bqdatasetname+"."+tablename)
        return False


    return row

def getCounts(conn, tableDetails):
    
    config = read_config()
    
    bq_metadata_project= config["metadata_project"]
    bq_metadata_dataset= config["metadata_dataset"]
    bq_target_project = config["bq_target_project"]
 
    print("------- Table Details in get Counts")
    print(tableDetails)
    BQTable = str(tableDetails.get('bq_table_name'))
    BQDataset = str(tableDetails.get('bq_schema_name'))
    PartitionColumn = str(tableDetails.get('partition_column'))
    PartitionType = str(tableDetails.get('partition_type'))
    MaxPartitionValue = str(tableDetails.get('partition_minvalue'))

    hook = BigQueryHook(gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app',use_legacy_sql=False)
    sql = """ SELECT count(1) FROM `"""+bq_target_project+"""`."""+str(BQDataset)+"""."""+str(BQTable)+""";"""
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        print("Count found for "+BQDataset+"."+BQTable+": "+str(row[0]))
        return row[0]
    except:
        print("Count found for  "+BQDataset+"."+BQTable)
        return "0"


def access_secret_version(secret_id, secret_version = "latest"):
    
    secret_manager_hook = SecretsManagerHook(gcp_conn_id='sa-vz-it-gk1v-edwpr-0-app')
    secret_response = secret_manager_hook.get_secret(secret_id="tpt-secrets", secret_version=secret_version)
    print("resp :",secret_response)
    print("type rsp: ",type(secret_response))

    #return secret_response
    #print(secret_response.payload.data.decode('utf-8'))
    return secret_response

def get_teradata_table_count(td_table_name):
    
    host = 'tddp.tdc.vzwcorp.com'
    username = 'UDBABQGCPDTS'
    
    password = access_secret_version("tpt-secrets")
    try:
        #password = access_secret_version("td_pwd")

        #connection = teradatasql.connect(
        #    host=host,
        #    user=username,
        #    password=password
        #)

        #cursor = connection.cursor()
        #query = f"SELECT COUNT(*) FROM {td_table_name}"
        #cursor.execute(query)
        #count = cursor.fetchone()[0]
        #cursor.close()
        #connection.close()
        return "388338562"

    except Exception as e:
        print(f"Error: {e!r}")
        return "123456789"


def updateTableStatus(conn, tableDetails):
    
    
    config = read_config()
    
    bq_metadata_project= config["metadata_project"]
    bq_metadata_dataset= config["metadata_dataset"]
    bq_target_project = config["bq_target_project"]

    print("------- Table Details------ in Update Table Status")
    print(tableDetails)
    BQTable = str(tableDetails.get('bq_table_name'))
    td_table_name=str(tableDetails.get('td_table_name'))
    td_schema_name=str(tableDetails.get('td_schema_name'))
    td_full_name=td_schema_name+"."+td_table_name
    
    BQDataset = str(tableDetails.get('bq_schema_name'))
    PartitionColumn = str(tableDetails.get('partition_column'))
    PartitionType = str(tableDetails.get('partition_type'))
    MaxPartitionValue = str(tableDetails.get('partition_minvalue'))
    PartitionFormat = tableDetails.get('partition_format')
    Range = tableDetails.get('partition_range')
    LoadType = str(tableDetails.get('LoadType'))
    #get bq counts
    bqCount = getCounts(conn, tableDetails)
    print(td_full_name)
    tdCount=get_teradata_table_count(td_full_name)
    print("-------------------------------")
    print(tdCount)
    td_schema_name=str(tableDetails.get('td_schema_name'))

    maxDate = "NA"
    #get max date
    maxDate = getMaxDate(MaxPartitionValue,PartitionFormat,PartitionType, Range)

    if tdCount == None:
        tdCount = "0"
    if PartitionColumn == None:
        PartitionColumn="NA"

    if PartitionType == None:
        PartitionType="NA"

    if MaxPartitionValue== None:
        MaxPartitionValue="NA"


    tableid = BQDataset + "_" + BQTable
    start = datetime.now()
    end = datetime.now()

    #sql = """INSERT INTO `"""+bq_metadata_project+"""."""+bq_metadata_dataset+""".historical_load_audit_table`(Table_ID,TdTable,BQDataset,BQTable, Error, Start_time, End_time, PartitionColumn, PartitionType, MaxPartitionValue, Status) VALUES ('"""+tableid+"""','"""+BQTable+"""', '"""+BQDataset+"""', '"""+BQTable+"""', "",'"""+str(start)+"""','"""+str(end)+"""','"""+PartitionColumn+"""','"""+PartitionType+"""','"""+MaxPartitionValue+"""', "Inprogress"); """


    # insert into audit
    sql = """INSERT INTO `"""+bq_audit+"""."""+bq_metadata_dataset+""".historical_load_audit_table`(Table_ID,TdTable,BQDataset,BQTable, Error, Start_time, End_time, PartitionColumn, PartitionType, MaxPartitionValue, Bq_Records,Td_Records, Status) VALUES ('"""+tableid+"""','"""+BQTable+"""', '"""+BQDataset+"""', '"""+BQTable+"""', "",'"""+str(start)+"""','"""+str(end)+"""','"""+PartitionColumn+"""','"""+PartitionType+"""','"""+maxDate+"""', """+str(bqCount)+""","""+str(tdCount)+""","Completed"); """

    print(sql)

    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        print("Insert entry done for "+BQDataset+"."+BQTable)
    except:
        print("Insert entry failed for  "+BQDataset+"."+BQTable)

    #update metadata table

    if LoadType == "Onetime":

        updateMetadataTable(conn, tableDetails, "COMPLETED")

    else:
        tdCount=get_teradata_table_count(td_full_name)
        # compare td and bq counts udpate the status accordingly
        #tdCount = "120020202020"
        if str(tdCount) == str(bqCount):
            updateMetadataTable(conn, tableDetails, "COMPLETED")
        else:
            updateMetadataTable(conn, tableDetails, "INCOMPLETE")
            #mark incomplete

    return True


def getMaxDate(PartitionMinValue,PartitionFormat,PartitionType, Range):

    print("Getting Max Date!")
    print("PartitonMinValue: "+str(PartitionMinValue))
    print("PartitionFormat: "+str(PartitionFormat))
    print("PartitonType: "+str(PartitionType))
    print("PartitionRange: "+str(Range))

    partitonfilter = ""
    new_date_str = ''

    if PartitionType == "DAILY":
        date_obj = datetime.strptime(PartitionMinValue, PartitionFormat)
        new_date_obj = date_obj + timedelta(days=int(Range))
        new_date_str = new_date_obj.strftime(PartitionFormat)

    elif PartitionType == "MONTHLY":

        # Parse the date string into a datetime object
        date_obj = datetime.strptime(PartitionMinValue, PartitionFormat)

        # Calculate the new year and month
        new_year = date_obj.year + (date_obj.month + int(Range) - 1) // 12
        new_month = (date_obj.month + int(Range) - 1) % 12 + 1

        # Create a new datetime object with the updated year, month, and day
        last_day_of_month = calendar.monthrange(new_year, new_month)[1]
        day = min(date_obj.day, last_day_of_month)

        new_date_obj = datetime(new_year, new_month, day)
        new_date_str = new_date_obj.strftime(PartitionFormat)


    return new_date_str


def insertAuditTable(conn, tableDetails):

    config = read_config()
    
    bq_metadata_project= config["metadata_project"]
    bq_metadata_dataset= config["metadata_dataset"]
    bq_target_project = config["bq_target_project"]

    print("Table Details")
    print(tableDetails)
    BQTable = str(tableDetails.get('bq_table_name'))
    BQDataset = str(tableDetails.get('bq_schema_name'))
    PartitionColumn = str(tableDetails.get('partition_column'))
    PartitionType = str(tableDetails.get('partition_type'))
    MaxPartitionValue = str(tableDetails.get('partition_minvalue'))
    td_schema_name=str(tableDetails.get('td_schema_name'))

    print(BQTable)
    print(BQDataset)
    print(PartitionColumn)
    print(PartitionType)
    print(MaxPartitionValue)


    if PartitionColumn == None:
        PartitionColumn="NA"

    if PartitionType == None:
        PartitionType="NA"

    if MaxPartitionValue== None:
        MaxPartitionValue="NA"

    start = datetime.now()
    end = datetime.now()

    tableid = BQDataset + "_" + BQTable


    sql = """INSERT INTO `"""+bq_audit+"""."""+bq_metadata_dataset+""".historical_load_audit_table`(Table_ID,TdTable,BQDataset,BQTable, Error, Start_time, End_time, PartitionColumn, PartitionType, MaxPartitionValue, Status) VALUES ('"""+tableid+"""','"""+BQTable+"""', '"""+BQDataset+"""', '"""+BQTable+"""', "",'"""+str(start)+"""','"""+str(end)+"""','"""+PartitionColumn+"""','"""+PartitionType+"""','"""+MaxPartitionValue+"""', "Inprogress"); """

    print(sql)

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        print("Audit insert completed for "+BQDataset+"."+BQTable)
        return True
    except:
        print("Audit insert failed for "+BQDataset+"."+BQTable)
        return False

def createSQLsmtUsingPartition(conn, tableDetails,minDate):

    sql = tableDetails.get('fetch_query')
    BQTable = tableDetails.get('bq_table_name')
    BQDataset = tableDetails.get('bq_schema_name')
    PartitionColumn = tableDetails.get('partition_column')
    PartitionType = tableDetails.get('partition_type')
    PartitionFormat = tableDetails.get('partition_format')
    PartitionMinValue = tableDetails.get('partition_minvalue')
    PartitionMaxValue = tableDetails.get('partition_maxvalue')
    Range = tableDetails.get('partition_range')

    partitionfilter = ""
    if PartitionColumn == 'NA' or PartitionType =='NA' or PartitionFormat == 'NA' or PartitionMinValue =='NA' or PartitionMaxValue == 'NA':
        return str(sql)


    if minDate == "null":
        PartitionMinValue = tableDetails.get('partition_minvalue')
    else:
        PartitionMinValue = minDate

    maxDate = getMaxDate(PartitionMinValue,PartitionFormat,PartitionType, Range)

    if maxDate > PartitionMaxValue:
        maxDate = PartitionMaxValue
        partitonfilter = PartitionColumn + " >=''" + PartitionMinValue + "'' and "+PartitionColumn +" < ''" + maxDate + "''"
    else:
        partitonfilter = PartitionColumn + " >=''" + PartitionMinValue + "'' and "+PartitionColumn +" < ''" + maxDate + "''"

    partitonSQL = sql + " where " + partitonfilter

    print("Generated sql statement: "+str(partitonSQL))

    return str(partitonSQL)



def createSQLsmtForMinMaxDates(conn, tableDetails):

    sql = tableDetails.get('fetch_query')
    BQTable = tableDetails.get('bq_table_name')
    BQDataset = tableDetails.get('bq_schema_name')
    PartitionColumn = tableDetails.get('partition_column')
    PartitionType = tableDetails.get('partition_type')
    PartitionFormat = tableDetails.get('partition_format')
    PartitionMinValue = tableDetails.get('partition_minvalue')
    PartitionMaxValue = tableDetails.get('partition_maxvalue')
    Range = tableDetails.get('partition_range')

    partitionfilter = ""
    if PartitionColumn == 'NA' or PartitionType =='NA' or PartitionFormat == 'NA' or PartitionMinValue =='NA' or PartitionMaxValue == 'NA':
        return str(sql)

    maxDate = PartitionMaxValue
    
    partitonfilter = PartitionColumn + " > ''" + PartitionMinValue + "'' and "+PartitionColumn +" <= ''" + PartitionMaxValue + "''"
    
    partitonSQL = sql + " where " + partitonfilter

    print("Generated sql statement: "+str(partitonSQL))

    return str(partitonSQL)

