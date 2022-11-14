# -*- coding: utf-8 -*-
"""
Created on Fri Jul 15 11:06:52 2022

@author: 171644
"""

import pandas as pd
import pyodbc
import pymysql
from sqlalchemy import create_engine, text as sa_text
from datetime import datetime as dt, date, timedelta
import calendar
import sys
from azure.storage.blob import BlockBlobService




#get batch id
def mysql_get_batchid(host, user, password):
    batchid_sql = "SELECT MAX(batch_id) FROM eaProcessControl.log_wideorbit_hmx_process;" 
    try:
        con = pymysql.connect(host=host,
                                user=user,
                                password=password,
                                autocommit=True,
                                local_infile=1)
        print('Connected to DB: {}'.format(host))
        # Create cursor and execute Load SQL
        cursor = con.cursor()
        cursor.execute(batchid_sql)
        batch_id = cursor.fetchall()
        con.close()
        return str(int(batch_id[0][0]) + 1)
        
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)
        

#write job status
def mysql_write_jobstatus(batch_id, status, job_start_dt, job_finish_dt, filename, error):
    print(status)
    status_sql = "INSERT INTO eaProcessControl.log_wideorbit_hmx_process (\
        batch_id, source, status, job_start_dt, job_finish_dt, filename, error)\
        values (" + batch_id + ", '" + source + "', '" + status + "', '" + job_start_dt + "', '" + job_finish_dt + "', '" + filename + "', '" + error + "');"
    print(status_sql)
    try:
        con = pymysql.connect(host = mysqlhost,
                                user = mysqluser,
                                password = mysqlpassw,
                                autocommit = True,
                                local_infile = 1)
        print('Connected to DB: {}'.format(mysqlhost))
        # Create cursor and execute Load SQL
        cursor = con.cursor()
        cursor.execute(status_sql)
        con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)
        

def write_to_blob(filename,to_file):
    """connects to azure and submits the file(s) to a blob"""
    
    print('Writing data to blob...')
    storage_account_name = 'account_name'
    storage_account_key = 'account_key'
    block_blob_service = BlockBlobService(account_name=storage_account_name
                                              , account_key=storage_account_key)
    try:
        block_blob_service.create_blob_from_text('sources/qa-report-automation', filename, to_file)
        print('Data has been written to the blob!')
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)


def write_to_snowflake(stmt_sql):
    """connects to snowflake and sumbits the provided sql statement"""
    
    print('Sending the statement to snowflake...')
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse=it_datateam_wh'.format(
        database='sfdvdh',
        schema='qa_report_automation',
        user='etl_management_svc',
        password='password',
        account='account'
            )
        )
    try:
        connection = engine.connect()
        print('connected to snowflake')
        connection.execute(sa_text(stmt_sql).execution_options(autocommit=True))
        connection.close()
        print('success!')
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)


def get_qa_report(stmt_sql):
  """connects to msserver and gets station spots info"""

    global df
    try:
      conn = pyodbc.connect('DRIVER={SQL Server}; \
                                SERVER=' + msserver + '; \
                                DATABASE=' + msdatabase + '; \
                                UID=' + msuser +'; \
                                PWD=' + mspassw)
      print('Connected to DB: {}'.format(msserver))
      # Create cursor and execute Load SQL
      df = df.append(pd.read_sql_query(stmt_sql,conn))
      return df
    except Exception as e:
      print('Error: {}'.format(str(e)))
      sys.exit(1)
      
      
def fix_columns(df,columns,column_order):
    df.columns = df.columns.str.upper()
    df.rename(columns=column_name_fix,inplace=True)
    df = df[column_order]
    return df

def get_wo_date_table(current_year):
  stmt_sql = f"SELECT SUBSTRING(DISPLAY_NAME, 1, 3) AS MONTHNAME, \
  DISPLAY_NAME, MONTH_NUMBER, YEAR_NUMBER, MONTH_START_DATE  \
  FROM WO_CALENDAR_MONTHS  \
  WHERE    CALENDAR_INT = 149 \
  AND YEAR_NUMBER in ( {current_year} ) ORDER BY MONTH_START_DATE;"
  try:
      conn = pyodbc.connect('DRIVER={SQL Server}; \
                                SERVER=' + msserver + '; \
                                DATABASE=' + msdatabase + '; \
                                UID=' + msuser +'; \
                                PWD=' + mspassw)
      print('Connected to DB: {}'.format(msserver))
      # Create cursor and execute Load SQL
      wo_dates = pd.read_sql_query(stmt_sql,conn)
      return wo_dates
  except Exception as e:
      print('Error: {}'.format(str(e)))
      sys.exit(1)
      
   

def loop_sql(station_ints,month_number,current_year):
    global df
    for station_int in station_ints:
        print(f'Getting station {station_int}')
        stmt_sql =     f"set nocount on \
         declare @ici table  \
         ( \
          inventory_code_int int  \
         );  \
         with InventorySubset as \
         (select INVENTORY_CODE_INT  \
         from WO_INVENTORY_CODES  \
         where (INVENTORY_CODE_INT in (13634,10711,30,11697,17132,17388,13635,2474,17766,17315,17832,9861,17320,17316,17317,17318, \
          17319,16211,16210,17,26,32,34,9451,36,5517,46,42,45,9271,9272,9273,27,38,47,48,39,49,50,29,7370,15860,9039,5521,2199,9452, \
          9453,9010,18,59,60,61,62,63,8774,10407,19,64,65,20,68,69,72,73,5218,5518,22,74,16197,80,81,82,84,85,2588,8408,86,10080,75,87, \
          88,89,90,91,83,92,93,31,5522,9007,10406,11919,14686,23,95,24,8309,25,111,112,113,8668,5519,9274,11743))  \
         )  \
         insert into @ici  \
           select distinct DESCENDANT_inventory_code_int as inventory_code_int  \
           from WO_INVENTORY_CODE_RELATIONSHIPS  \
           where  \
             ANCESTOR_inventory_code_int in (select * from InventorySubset)  \
             or  \
             ANCESTOR_inventory_code_int in   (  \
                select LINKED_inventory_code_int  \
                from WO_INVENTORY_CODE_SELLGROUPS  \
                where inventory_code_int in (select * from InventorySubset)    ) \
                \
                \
                \
        declare @calendar int \
         set @calendar = 149 \
         \
         set nocount on \
         declare @cm table \
         ( \
          month_number int, \
          year_number int, \
          quarter_number int, \
          month_start_date smalldatetime, \
          month_end_date smalldatetime \
         ) \
          insert into @cm \
            select \
              month_number, \
              year_number, \
              quarter_number, \
              month_start_date, \
              month_end_date \
          from wo_calendar_months WITH (NOLOCK)  \
          where \
            calendar_int = @calendar and year_number in ({current_year}) \
         \
         declare @min_date smalldatetime \
         declare @max_date smalldatetime \
          select \
            @min_date  = min(month_start_date), \
            @max_date  = max(month_end_date) \
          from @cm \
          \
          \
        select A.* from ( select \
          cast(sum(revenue_total_gross) as money) as revenue_total_gross, \
          month_number, year_number, quarter_number,  \
          STATION_GROUP_NAME,CHANNEL_NAME,ADVERTISER_NAME,ORDER_NUMBER,INTENDED_AIR_DATE,ALT_ORDER_NUMBER,INVENTORY_CODE,REVENUE_CODE1,REVENUE_CODE2,REVENUE_CODE3,billing_calendar_name \
          ,channel_int, station_int, cast( channel_array as image) channel_array \
        from ( \
          (select  \
          cm.Month_number,  \
          cm.Year_number,  \
          cm.Quarter_number  \
         \
          ,cast(ROUND(sum(olu.internal_amount * ISNULL(oae.COMMISSION_PERCENT / 100, 1)), 2) as money) as revenue_total_gross  \
          ,st.station_int \
          ,isnull(oluc.channel_array, olc.channel_array) as channel_array \
          ,ch.channel_int \
          ,luStationGroup.lookup_display as STATION_GROUP_NAME \
           ,case ch.REPORTING_NAME \
             when null then 'All'  \
             when '' then ch.CHANNEL_NAME  \
             else ch.CHANNEL_NAME + '/' + ch.REPORTING_NAME \
           end as channel_name \
          ,advertiser.advertiser_name \
          ,ord.order_number \
          ,olu.intended_air_date \
          ,ord.alt_order_number \
          ,inv_cd.Inventory_code_name as Inventory_code \
          ,luRevenueCode1.lookup_value as Revenue_Code1 \
          ,luRevenueCode2.lookup_value as Revenue_Code2 \
          ,luRevenueCode3.lookup_value as Revenue_Code3 \
            ,luBillingCal.lookup_display as billing_calendar_name \
        from  \
          wo_orders ord WITH (NOLOCK)  \
          inner join wo_order_bill_plans obp WITH (NOLOCK) on ord.order_int = obp.order_int \
            and (obp.USE_SINGLE_PLAN_PER_SPOT = 1) \
          inner join  \
             (select INTERNAL_AMOUNT, 1 as count_spot, REVENUE_CODE_INT, REVENUE_CODE2_INT, REVENUE_CODE3_INT, UNIT_CODE_INT, OVERRIDE_BREAK_CODE_INT, \
              override_inventory_code_int,  ORDER_LINE_UNIT_ID  ,ORDER_LINE_ID  ,ORDER_INT  ,STATION_INT  ,CHANNEL_INT  ,SEQ_NUM  ,ORDER_LINE_SCHEDULE_ID  , \
              INVENTORY_TYPE_INT  ,SELLING_ELEMENT_INT  ,INVENTORY_CODE_INT  ,BREAK_CODE_INT  ,BREAK_POSITION  ,BREAK_NUMBER  ,PRIORITY_CODE_INT  ,UNIT_AIR_STATUS_CODE_INT  , \
              AIR_TIME1  ,AIR_TIME2  ,AIR_LENGTH1  ,AIR_LENGTH2  ,MATERIAL_INT1  ,MATERIAL_INT2  ,PRINT_ON_ORDER  ,PRINT_ON_INVOICE  ,INVOICE_INT  ,INVOICE_ISCI_CODE1  ,INVOICE_ISCI_CODE2  , \
              AGREED_INVENTORY_RATING  ,INTENDED_AIR_DATE  ,FIRST_ELIGIBLE_AIR_DATE  ,LAST_ELIGIBLE_AIR_DATE  ,INTENDED_LOG_INT  ,ELIGIBLE_WEEKDAYS  ,DISPLAYED_TIMES  ,INVENTORY_DESCR  , \
              RESTRICT_START_TIME  ,RESTRICT_END_TIME  ,IGNORE_INVENTORY_CODE  ,LENGTH1  ,LENGTH2  ,LIFECYCLE_STATE  ,IS_PLACED_REVENUE_ONLY  ,IS_PLACED  ,IS_LATE_ADD  ,IS_CREDITED  , \
              IS_USER_MODIFIED  ,IS_PENDING  ,IS_REJECTED  ,IS_PLACEABLE  ,WAS_MADEGOOD  ,IS_MAKEGOOD  ,IS_UNRESOLVED  ,MADE_GOOD_BY_DESCR  ,CURRENT_STATE_DESCR  ,IS_BONUS  , \
              advertiser_brand_int  ,EXTERNAL_AMOUNT  ,invoice_channel_override  ,REJECT_DATE  ,AGREED_IMPRESSIONS  ,PROGRAM_INT  ,AIRED_PROGRAM_INT  ,IS_CLEARED_CIA  from WO_OLU_{station_int} WITH (NOLOCK)  \
              union all  \
              select (external_amount - internal_amount) as internal_amount, 0 as count_spot, H_REVENUE_CODE1_INT, H_REVENUE_CODE2_INT, H_REVENUE_CODE3_INT,  \
              UNIT_CODE_INT, NULL AS OVERRIDE_BREAK_CODE_INT, null as override_inventory_code_int,  ORDER_LINE_UNIT_ID  ,ORDER_LINE_ID  ,ORDER_INT  ,STATION_INT  ,CHANNEL_INT  , \
              SEQ_NUM  ,ORDER_LINE_SCHEDULE_ID  ,INVENTORY_TYPE_INT  ,SELLING_ELEMENT_INT  ,INVENTORY_CODE_INT  ,BREAK_CODE_INT  ,BREAK_POSITION  ,BREAK_NUMBER  ,PRIORITY_CODE_INT  , \
              UNIT_AIR_STATUS_CODE_INT  ,AIR_TIME1  ,AIR_TIME2  ,AIR_LENGTH1  ,AIR_LENGTH2  ,MATERIAL_INT1  ,MATERIAL_INT2  ,PRINT_ON_ORDER  ,PRINT_ON_INVOICE  ,INVOICE_INT  , \
              INVOICE_ISCI_CODE1  ,INVOICE_ISCI_CODE2  ,AGREED_INVENTORY_RATING  ,INTENDED_AIR_DATE  ,FIRST_ELIGIBLE_AIR_DATE  ,LAST_ELIGIBLE_AIR_DATE  ,INTENDED_LOG_INT  ,ELIGIBLE_WEEKDAYS  , \
              DISPLAYED_TIMES  ,INVENTORY_DESCR  ,RESTRICT_START_TIME  ,RESTRICT_END_TIME  ,IGNORE_INVENTORY_CODE  ,LENGTH1  ,LENGTH2  ,LIFECYCLE_STATE  ,IS_PLACED_REVENUE_ONLY  ,IS_PLACED  , \
              IS_LATE_ADD  ,IS_CREDITED  ,IS_USER_MODIFIED  ,IS_PENDING  ,IS_REJECTED  ,IS_PLACEABLE  ,WAS_MADEGOOD  ,IS_MAKEGOOD  ,IS_UNRESOLVED  ,MADE_GOOD_BY_DESCR  ,CURRENT_STATE_DESCR  , \
              IS_BONUS  ,advertiser_brand_int  ,EXTERNAL_AMOUNT  ,invoice_channel_override  ,REJECT_DATE  ,AGREED_IMPRESSIONS  ,PROGRAM_INT  ,AIRED_PROGRAM_INT  , \
              IS_CLEARED_CIA  from WO_OLU_{station_int} WITH (NOLOCK) where internal_amount <> external_amount) as olu  \
             on ord.order_int = olu.order_int  \
                and olu.intended_air_date between obp.unit_start_date and obp.unit_end_date \
         \
          inner join wo_stations st WITH (NOLOCK) on st.station_id = ord.station_id \
          inner join wo_station_arrays stsa WITH (NOLOCK) on stsa.station_array_int = st.station_array_int \
          inner join @cm cm on olu.intended_air_date <= cm.month_end_date and  \
            olu.intended_air_date >= cm.month_start_date  \
          left outer join wo_Channels ch WITH (NOLOCK) on ch.channel_int = olu.channel_int  \
          left outer join wo_lookups luStationGroup WITH (NOLOCK) on luStationGroup.lookup_type = 'STATION_GROUP' and luStationGroup.lookup_int = st.station_group_int \
          inner join wo_order_account_execs oae WITH (NOLOCK) on oae.order_int = ord.order_int \
               and (oae.start_date is null or oae.start_date <= olu.intended_air_Date)  \
               and (oae.end_date is null or oae.end_date >= olu.intended_air_Date)  \
          inner join wo_advertisers advertiser WITH (NOLOCK) on advertiser.advertiser_id = obp.advertiser_id \
          inner join wo_lookups luRevenueCode1 WITH (NOLOCK) on luRevenueCode1.lookup_INT = olu.revenue_code_INT \
          inner join wo_lookups luRevenueCode2 WITH (NOLOCK) on luRevenueCode2.lookup_INT = olu.revenue_code2_INT \
          inner join wo_lookups luRevenueCode3 WITH (NOLOCK) on luRevenueCode3.lookup_INT = olu.revenue_code3_INT \
          inner join wo_lookups luBillingCal WITH (NOLOCK) ON  luBillingCal.lookup_int = ord.BILLING_CALENDAR_INT \
          inner join wo_inventory_codes inv_cd WITH (NOLOCK) on inv_cd.inventory_code_int = olu.inventory_code_int \
          inner join @ici ici on olu.inventory_code_int = ici.inventory_code_int \
         left outer join wo_order_line_channels olc WITH (NOLOCK) on olc.order_line_id = olu.order_line_id \
         left outer join wo_order_line_unit_channels oluc WITH (NOLOCK) on oluc.order_line_unit_id = olu.order_line_unit_id \
        where  \
          olu.INTENDED_AIR_DATE between @min_date and @max_date and  \
          ((olu.Is_CREDITED = 0) or (olu.is_unresolved <> 0))  \
         \
          and olu.internal_amount <> 0  \
         and (olu.IS_PENDING = 0)  \
         \
          and obp.ORDER_IS_CASH <> 0  \
          and (olu.IS_Rejected = 0) and (olu.is_unresolved = 0) \
          and ord.station_id in (select station_id from wo_stations where station_int = {station_int}) \
          and olu.station_int = {station_int} \
          and (olu.channel_int IS NULL OR olu.Channel_int IN (1,107,109,110,113,114,116,118,12,120,122,124,125,128,130,132,133,135,137,139, \
            14,142,143,144,150,153,156,159,16,160,165,167,168,18,19,22,25,26,28,30,32,35,37,39,4,41,45,48,50,51,52,56,58,6,60,61,62,65,66,9)) \
          and cm.year_number in ({current_year})  \
          and cm.month_number in ({month_number})  \
        group by  \
          cm.month_number,  \
          cm.year_number,  \
          cm.quarter_number  \
         \
           ,st.station_int \
           ,isnull(oluc.channel_array, olc.channel_array) \
          ,ch.channel_int \
          ,luStationGroup.lookup_display \
          ,ch.channel_name \
          ,ch.reporting_name \
          ,advertiser.advertiser_name \
          ,ord.order_number \
          ,olu.intended_air_date \
          ,ord.alt_order_number \
          ,inv_cd.Inventory_code_name \
          ,luRevenueCode1.lookup_value \
          ,luRevenueCode2.lookup_value \
          ,luRevenueCode3.lookup_value \
          ,luBillingCal.lookup_display \
        )  \
        union all  \
          ( /* u_RevenueReportSQL.BaseSelectAsSpotRevenueHistory */ select  \
          cm.MONTH_NUMBER,  \
          cm.YEAR_NUMBER,  \
          cm.QUARTER_NUMBER  \
         \
          ,sum(rdr.gross * ISNULL(oae.COMMISSION_PERCENT / 100, 1)) \
            as revenue_total_gross \
          ,rdr.station_int \
          ,null as channel_array \
          ,ch.channel_int \
          ,luStationGroup.lookup_display as STATION_GROUP_NAME \
           ,case ch.REPORTING_NAME \
             when null then 'All'  \
             when '' then ch.CHANNEL_NAME \
             else ch.CHANNEL_NAME + '/' + ch.REPORTING_NAME \
           end as channel_name \
          ,advertiser.advertiser_name \
          ,isNull(ord.order_number, 'n/a') as order_number \
          ,rdr.intended_air_date \
          ,isNull(ord.alt_order_number, 'n/a') as alt_order_number \
          ,'n/a' as Inventory_code \
          ,luRevenueCode1.lookup_value as Revenue_Code1 \
          ,luRevenueCode2.lookup_value as Revenue_Code2 \
          ,luRevenueCode3.lookup_value as Revenue_Code3 \
            ,luBillingCal.lookup_display as billing_calendar_name \
        from  \
          wo_report_data_revenue rdr WITH (NOLOCK)  \
          inner join @cm cm on rdr.intended_air_date <= cm.month_end_date and  \
            rdr.intended_air_date >= cm.month_start_date  \
         \
          inner join wo_stations st WITH (NOLOCK) on st.station_int = rdr.station_int \
          inner join wo_station_arrays stsa WITH (NOLOCK) on stsa.station_array_int = st.station_array_int \
          left outer join wo_orders ord WITH (NOLOCK) on ord.order_id = rdr.order_id \
          left outer join wo_channels ch WITH (NOLOCK) on ch.channel_int = rdr.channel_int \
          inner join wo_advertisers advertiser WITH (NOLOCK) on advertiser.advertiser_id = rdr.advertiser_id \
          inner join wo_lookups luRevenueCode1 WITH (NOLOCK) on luRevenueCode1.lookup_INT = rdr.revenue_code_INT \
          inner join wo_lookups luRevenueCode2 WITH (NOLOCK) on luRevenueCode2.lookup_INT = rdr.revenue_code2_INT \
          inner join wo_lookups luRevenueCode3 WITH (NOLOCK) on luRevenueCode3.lookup_INT = rdr.revenue_code3_INT \
          left outer join wo_order_account_execs oae WITH (NOLOCK) on oae.order_int = ord.order_int \
               and (oae.start_date is null or oae.start_date <= rdr.intended_air_Date)  \
               and (oae.end_date is null or oae.end_date >= rdr.intended_air_Date)  \
          left outer join wo_account_execs ae WITH (NOLOCK)  on (ae.account_exec_id = case when rdr.order_id is not null then oae.account_exec_id else rdr.account_exec_id end) \
          left outer join wo_lookups luBillingCal WITH (NOLOCK) ON  luBillingCal.lookup_int = ord.billing_calendar_int \
          left outer join wo_lookups luStationGroup WITH (NOLOCK) on luStationGroup.lookup_type = 'STATION_GROUP' and luStationGroup.lookup_int = st.station_group_int \
        where  \
          rdr.INTENDED_AIR_DATE between @min_date and @max_date and \
          rdr.row_state <> 4  \
         \
          and rdr.AS_TRADE = 'N'  \
          and (rdr.payment_id is null) and (rdr.revenue_type <> 1)  \
          and (rdr.payment_id is not null)  \
          and rdr.station_int = {station_int} \
          and (ch.channel_int is null or ch.Channel_int in (1,107,109,110,113,114,116,118,12,120,122,124,125,128,130,132,133,135,137,139,14, \
            142,143,144,150,153,156,159,16,160,165,167,168,18,19,22,25,26,28,30,32,35,37,39,4,41,45,48,50,51,52,56,58,6,60,61,62,65,66,9 )) \
          and cm.year_number in ({current_year})  \
          and cm.month_number in ({month_number})  \
        group by  \
          cm.MONTH_NUMBER,  \
          cm.YEAR_NUMBER,  \
          cm.QUARTER_NUMBER  \
         \
          ,ord.order_id  \
          ,rdr.station_int \
          ,ch.channel_int \
          ,luStationGroup.lookup_display \
          ,ch.channel_name \
          ,ch.reporting_name \
          ,advertiser.advertiser_name \
          ,ord.order_number \
          ,rdr.intended_air_date \
          ,ord.alt_order_number \
          ,luRevenueCode1.lookup_value \
          ,luRevenueCode2.lookup_value \
          ,luRevenueCode3.lookup_value \
          ,luBillingCal.lookup_display \
        ) ) DERIVEDTBL  \
        group by  \
          month_number, year_number, quarter_number  \
          ,STATION_GROUP_NAME,CHANNEL_NAME,ADVERTISER_NAME,ORDER_NUMBER,INTENDED_AIR_DATE,ALT_ORDER_NUMBER,INVENTORY_CODE,REVENUE_CODE1,REVENUE_CODE2,REVENUE_CODE3,billing_calendar_name \
          ,channel_int, station_int, channel_array  \
         )A  \
         \
        order by \
          STATION_GROUP_NAME,CHANNEL_NAME,ADVERTISER_NAME,ORDER_NUMBER,INTENDED_AIR_DATE,ALT_ORDER_NUMBER,INVENTORY_CODE,REVENUE_CODE1,REVENUE_CODE2,REVENUE_CODE3,billing_calendar_name, \
          channel_int, station_int;" 
       
        
        df = get_qa_report(stmt_sql)
    return df


def main():
  #mssql params
  msserver = 'server'
  msdatabase = 'traffic' 
  msuser = 'user' 
  mspassw = 'password'

  #set date info
  today = date.today().strftime('%Y-%m-%d')
  current_month = dt.now().strftime('%h')
  current_year = date.today().year
  wo_dates = get_wo_date_table(current_year)  
  month_number = wo_dates.loc[wo_dates['MONTHNAME'] == f'{current_month}', 'MONTH_NUMBER'].item()
  month = calendar.monthcalendar(current_year, month_number)
  last_sunday = '{}-{:02}-{}'.format(current_year,month_number,max(month[-1][calendar.SUNDAY], month[-2][calendar.SUNDAY]))

  #stations list
  station_ints = [1,10,108,110,113,114,116,118,119,121,123,125,126,128,13,131,133,135,136,137,140,
  15,150,153,156,159,162,163,165,168,17,19,2,20,23,26,28,29,31,33,36,38,41,45,48,5,50,
  51,54,56,58,59,60,64,66,67,7]

  #column janitorial work
  column_name_fix = {'REVENUE_TOTAL_GROSS':'DOLLARS','STATION_GROUP_NAME':'PROPERTY_GROUP','CHANNEL_NAME':'CHANNELS','ADVERTISER_NAME':'ADVERTISERS','ORDER_NUMBER':'ORDER_NO',
                          'INTENDED_AIR_DATE':'AIR_DATE','ALT_ORDER_NUMBER':'ALT_ORDER_NO','REVENUE_CODE1':'REV_CODE_1','REVENUE_CODE2':'REV_CODE_2','REVENUE_CODE3':'REV_CODE_3',
                          'BILLING_CALENDAR_NAME':'BILLING_CALENDAR'}

  column_order = ['PROPERTY_GROUP','CHANNELS','ADVERTISERS','ORDER_NO','BILLING_CALENDAR','AIR_DATE','ALT_ORDER_NO','REV_CODE_1','REV_CODE_2','REV_CODE_3','INVENTORY_CODE','DOLLARS','MONTH_NUMBER']

  #make the df and fix the columns
  df = pd.DataFrame()
  df = loop_sql(station_ints,month_number,current_year)
  df = fix_columns(df, column_name_fix, column_order)

  #assign the filename and write to blob
  filename = f'qa_report_{today}.csv'
  to_file = df.to_csv(index=False) 
  write_to_blob(filename,to_file)

  #delete all current month records from coding review
  stmt_sql = f"delete from SFDVDH.QA_REPORT_AUTOMATION.CODING_REVIEW where \
  month_number = {month_number} \
  and left(AIR_DATE,4) = {current_year};"

  write_to_snowflake(stmt_sql)

  #copy df into coding review
  stmt_sql = f'copy into SFDVDH.QA_REPORT_AUTOMATION.CODING_REVIEW from \
  (select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, \
  current_timestamp(), metadata$filename \
  from @SFDVDH.QA_REPORT_AUTOMATION.qa_report_automation_stage) \
  file_format = (format_name = SFDVDH.QA_REPORT_AUTOMATION.CODING_REVIEW_CSV) \
  pattern =".*qa_report_{today}.csv";'

  write_to_snowflake(stmt_sql)

  #truncate flagged errors
  truncate_stmt = 'truncate table SFDVDH.QA_REPORT_AUTOMATION.FLAGGED_ERRORS;'

  write_to_snowflake(truncate_stmt)

  #insert into flagged errors from coding review
  stmt_sql = f"insert into SFDVDH.QA_REPORT_AUTOMATION.FLAGGED_ERRORS  \
  select * from SFDVDH.QA_REPORT_AUTOMATION.CODING_REVIEW \
  where rev_code_1 = 'NON-SLS' and rev_code_2 != 'NON-SLS' \
  or rev_code_1 != 'NON-SLS' and rev_code_2 = 'NON-SLS' \
  or rev_code_1 not in ('DIR', 'DISC', 'NON-SLS') and advertisers not in ('Digital Promotion', 'NBC Network Promotion', 'NBC Network Promotion - Direct', 'CW Promotions') \
  or rev_code_2 not in ('NON-SPOT DIGITAL ONLY', 'NON-SLS', 'POL') and rev_code_2 not like 'EWSD%' \
  or rev_code_3 = '3rd Party Promo' and advertisers not in ('Digital Promotions', 'NBC Network Promotion', 'CW Promotions',' CW-Direct Promotions') \
  or rev_code_3 = 'EVOCA' and advertisers != 'Edge Networks Inc dba Evoca - Direct' \
  or rev_code_3 = 'Int Dis- 3rd Party' and inventory_code not in ('Aud Ext. 300+728', 'Aud Ext. 300+728+160+Mob', 'Aud Ext-G 300+728+160+Mob',  \
  'Aud Ext. Audio', 'Aud Ext. Display Ads', 'Aud Ext-G Display Ads', 'Aud Ext-G-YT',  \
  'Facebook & Instagram', 'Facebook Boost', 'Local Segment FB Boost', 'PPC Facebook & Instagram',  \
  'Sponsored Station YouTube', 'STN Hyperlocal', 'Video - STN', 'Weather.com') \
   or rev_code_3 = 'Int Dis-TVstation.com' and inventory_code not in ('Adhesion 320x50', 'Adhesion 728x90', 'Adhesion 728x90 + 320x50', 'Banner Ad 300x250', 'Banner Ad 300x600', \
  'Banner Ad 300x600+Logo', 'Banner Ad 728x90', 'Banner Ad 970x250', 'Brand Spotlight', 'Brand Spotlight Content',  \
  'Brand Spotlight Teaser', 'Display Ads -All Ad Sizes', 'Mobile', 'Pencil Pushdown', 'Pencil, 300 + 728',  \
  'Pencil, 300x250', 'Scripps Video', 'Skin-Topper-Wall+300+728', 'Skin-Topper-Wallpaper', 'Skin-Topper-Wallpaper+300',  \
  'Sponsored Logo 300x80') \
  or rev_code_3 = 'Int Mes-3rd Party' and inventory_code != 'Email Marketing' \
  or rev_code_3 = 'Int Mes-TVstation.com' and inventory_code not in ( 'Contest Station FB Post', 'Email Newsletter', 'Local Segment FB Post', 'Sponsored Email (No Ads)',  \
   'Station Facebook Post', 'Station Twitter Post', 'Web Chat') \
  or rev_code_3 = 'Int Part-SEM' and inventory_code != 'PPC Google Yahoo Bing' \
  or rev_code_3 = 'Int Prod-3rd Party' and inventory_code not in ('Ad Creation-3rd Party', 'Call Tracking', 'Land Pg-3rd Party Created',  \
   'SEO Services', 'Video Ad Production', 'Website Hosting', 'Brand Spotlight Content') \
  or rev_code_3 = 'Int Prod-Scripps' and inventory_code not in ('Ad Creation-Scripps', 'Deals', 'Landing Page-Site Created', 'Local Segment Content',  \
   'Local Segment Content-ISF', 'National Segment Content', 'Production Fees', 'Brand Spotlight Content') \
  or rev_code_3 = 'Newsy - Local' and inventory_code != 'Newsy Video' \
  or rev_code_3 = 'OTT 3RD PARTY' and inventory_code not in ('Amazon-OTT Growth', 'Amazon-OTT Premium', 'Amazon-OTT Reach', 'Amazon-Twitch OTT Reach',  \
  'Double Verify Tag', 'Octane Verify', 'Scripps Octane', 'Scripps Octane OLV') \
  or rev_code_3 = 'OTT 3rd Party-High Contra' and inventory_code not in ('OTT-WRAL', 'Scripps Octane') \
  or rev_code_3 = 'OTT LOCAL' and inventory_code not in ('FL24', 'Livestream', 'Local Segment Content-OTT', 'Scripps Video-OTT', 'Video Network Livestream') \
  or rev_code_3 = 'OTT Madhive' and inventory_code != 'Scripps Video Lcl Madhive' \
  or month_number = {month_number} and billing_calendar = 'Broadcast' and air_date > '{last_sunday}' and alt_order_no is not NULL and (left(alt_order_no,6) = 'AdBook') \
  and month_number in (SELECT max(month_number) from SFDVDH.QA_REPORT_AUTOMATION.CODING_REVIEW);"

  write_to_snowflake(stmt_sql)

if __name__ == '__main__':
  main()

                                                           
