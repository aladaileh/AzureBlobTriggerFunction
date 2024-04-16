import azure.functions as func
import logging
from azure.functions.decorators.core import DataType
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json




app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="gasconsumption/inputs_df.csv",
                               connection="AzureWebJobsStorage") 
@app.blob_input(arg_name="inputblob", path="gasconsumption/inputs_df.csv",
                               connection="AzureWebJobsStorage")
@app.blob_input(arg_name="inputblob1", path="gasconsumption/AemoNomsAndForcastFlow.csv",
                               connection="AzureWebJobsStorage")
@app.blob_input(arg_name="inputblob2", path="gasconsumption/KHNPAvialability.csv",
                               connection="AzureWebJobsStorage")
@app.sql_input(arg_name="DefTable",
                        command_text="SELECT * FROM dbo.DefTable",
                        command_type="Text",
                        connection_string_setting="SqlConnectionString")
@app.generic_output_binding(arg_name="toDoItems", type="sql", CommandText="dbo.DefTable",
                             ConnectionStringSetting="SqlConnectionString",data_type=DataType.STRING)
def DefinitionTable(myblob: func.InputStream , inputblob: str, inputblob1: str,inputblob2: str ,
                   DefTable:  func.SqlRowList ,toDoItems: func.Out[func.SqlRow]):
    def send_email(smtp_server,port,sender,password,recipients,subject,body):
        msg=MIMEMultipart()
        msg['From']= sender
        msg['To']=','.join(recipients)
        msg['Subject']=subject
        msg.attach(MIMEText(body,'Plain'))
        server=smtplib.SMTP(smtp_server,port)
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)
        server.quit()

    smtp_server='smtp.gmail.com'
    port=587
    recipients = ['aadylih@gmail.com']
    password='dkenpftyzirukknz'
    sender='ahmadriad19971@gmail.com'
    subject="Deftable code"

    rows = list(map(lambda r: json.loads(r.to_json()), DefTable))
    existing_ids=pd.DataFrame(rows)

    try:
        if existing_ids.empty:

                    column_names=['CurveID','Location','Commodity','Period','Source','Senario','Type','SubType','SubSubTybe','Unit','HubType','FreeText','Timestamp']
                    existing_ids= pd.DataFrame(rows, columns=column_names)
   
        dbt=existing_ids.drop(['CurveID','Timestamp'], axis=1).fillna('')

        data = inputblob
        df0 = pd.read_csv(StringIO(data)).astype(object).fillna('')

        data1 = inputblob1
        df1 = pd.read_csv(StringIO(data1)).astype(object).fillna('')

        data2 = inputblob2
        df2 = pd.read_csv(StringIO(data2)).astype(object).fillna('')

        dfs = [df0, df1, df2]
        difftable=[]

        for df in dfs:

            df.drop(['Unnamed: 0','ValueDate','AsOfDate','Value','Timestamp'],axis=1, inplace=True)
            
            df=df.drop_duplicates()

            a = dbt.dtypes
            b = df.dtypes
            c= dbt.columns
            d=  df.columns
            if a.equals(b) and c.equals(d):
                print('All good')
            else:
                raise Exception('Data type or column mismatch detected. Please check your data.')
            difftable.append(df)

        combined_df = pd.concat(difftable, ignore_index=True)

        DefTable=  combined_df.astype(object).fillna('')
        
        dbt_df= dbt.merge(right=DefTable,on= ['Location','Commodity','Period','Source','Senario','Type','SubType','SubSubTybe','Unit','HubType','FreeText'],indicator=True, how ='outer')


        diff=dbt_df.loc[lambda x: x['_merge'] =='right_only']
        diff=diff.drop(['_merge'],axis=1)
        diff= diff.astype(str).fillna('')
        diff['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        toDoItems.set(
        [func.SqlRow(
            {"Location": row['Location'],
            "Commodity": row['Commodity'],
            "Period": row['Period'],
            "Source": row['Source'],
            "Senario": row['Senario'],
            "Timestamp": row['Timestamp'],
            "Type": row['Type'],
            "SubType": row['SubType'],
            "SubSubTybe": row['SubSubTybe'],
            "Unit": row['Unit'],
            "HubType": row['HubType'],
            "FreeText": row['FreeText']}
        ) for index, row in diff.iterrows()]
    )

        print(len(diff),diff)
        send_email(smtp_server, port, sender, password, recipients, subject,f'{len(diff)}')  
    except Exception as e:
        print(f"Error: {str(e)}")  
        send_email(smtp_server, port, sender, password, recipients, subject, str(e))  
        raise 



@app.blob_trigger(arg_name="myblob", path="gasconsumption/inputs_df.csv",
                               connection="AzureWebJobsStorage") 
@app.blob_input(arg_name="inputblob0", path="gasconsumption/inputs_df.csv",
                               connection="AzureWebJobsStorage")
@app.blob_input(arg_name="inputblob01", path="gasconsumption/AemoNomsAndForcastFlow.csv",
                               connection="AzureWebJobsStorage")
@app.blob_input(arg_name="inputblob02", path="gasconsumption/KHNPAvialability.csv",
                               connection="AzureWebJobsStorage")
@app.sql_input(arg_name="DefTable",
                        command_text="SELECT * FROM dbo.DefTable",
                        command_type="Text",
                        connection_string_setting="SqlConnectionString")
@app.sql_input(arg_name="TimeSeries",
                        command_text="SELECT * FROM dbo.TimeSeries",
                        command_type="Text",
                        connection_string_setting="SqlConnectionString")

@app.generic_output_binding(arg_name="toDoItems1", type="sql", CommandText="dbo.TimeSeries", ConnectionStringSetting="SqlConnectionString",data_type=DataType.STRING)
def TimeSeriesTable(myblob: func.InputStream, inputblob0: str, inputblob01: str, inputblob02: str,
                    DefTable:  func.SqlRowList , TimeSeries:  func.SqlRowList , toDoItems1: func.Out[func.SqlRow]) :
    def send_email(smtp_server,port,sender,password,recipients,subject,body):
        msg=MIMEMultipart()
        msg['From']= sender
        msg['To']=','.join(recipients)
        msg['Subject']=subject
        msg.attach(MIMEText(body,'Plain'))
        server=smtplib.SMTP(smtp_server,port)
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)
        server.quit()

    smtp_server='smtp.gmail.com'
    port=587
    recipients = ['aadylih@gmail.com']
    password='dkenpftyzirukknz'
    sender='ahmadriad19971@gmail.com'
    subject="(TimeSeriesTable)"

    try:

        rows = list(map(lambda r: json.loads(r.to_json()), DefTable))
        dbDT1=pd.DataFrame(rows)
        if dbDT1.empty:
                    column_names=['CurveID','Location','Commodity','Period','Source','Senario','Type','SubType','SubSubTybe','Unit','HubType','FreeText','Timestamp']
                    dbDT1= pd.DataFrame(rows, columns=column_names)
   

        rows2= list(map(lambda r: json.loads(r.to_json()), TimeSeries))
        dbt1=pd.DataFrame(rows2)
        if dbt1.empty:
                    column_names01=['CurveID','ValueDate','AsOfDate','Value','Timestamp']
                    dbt1= pd.DataFrame(rows, columns=column_names01)

        dbTT1=dbt1.drop(['Timestamp'],axis=1).fillna('')

        dbTT1['ValueDate'] = pd.to_datetime(dbTT1['ValueDate'])
        dbTT1['AsOfDate'] = pd.to_datetime(dbTT1['AsOfDate'])
        dbTT1['AsOfDate'] = dbTT1['AsOfDate'].dt.tz_localize(None)
        dbTT1['ValueDate'] = dbTT1['ValueDate'].dt.tz_localize(None)


        # dbTT1['AsOfDate'] = dbTT1['AsOfDate'].astype('datetime64[ns]')
        

         
        try:
            df0 = inputblob0
            data0 = pd.read_csv(StringIO(df0)).astype(object).fillna('')
            df01 = inputblob01
            data01 = pd.read_csv(StringIO(df01)).astype(object).fillna('')
            df02 = inputblob02
            data02 = pd.read_csv(StringIO(df02)).astype(object).fillna('')

            dfs= [data0, data01 ,data02]

        except Exception as e:
            raise print(e)
 

        timetable = []
        for data1 in dfs:
            date_columns = ['AsOfDate', 'ValueDate','Timestamp']
            data1[date_columns] = data1[date_columns].apply(pd.to_datetime)
            df1= dbDT1.merge(how='outer',on=['Location','Commodity','Period','Source','Senario','Type','SubType',
                                            'SubSubTybe','Unit','HubType','FreeText'], right=data1)
            df1.dropna(inplace=True)
            Timseries_data_table=df1.drop(['Unnamed: 0','Location','Commodity','Period','Source','Senario','Type','SubType',
                                        'SubSubTybe','Unit','HubType','FreeText','Timestamp_x','Timestamp_y'],axis=1)
            Timseries_data_table['CurveID'] = Timseries_data_table['CurveID'].astype('int64')

            v = dbTT1.dtypes
            x = Timseries_data_table.dtypes
            y= dbTT1.columns
            z= Timseries_data_table.columns
            # print(x)
            # print(v)

            if v.equals(x) and y.equals(z):
                print('All good')
            else:
                try:
                    dbTT1['Value'] = pd.to_numeric(dbTT1['Value'], errors='coerce')

                except:
                    
                    raise Exception('Data type or column mismatch detected. Please check your data.')
          
          
            timetable.append(Timseries_data_table)


       
        combined_df = pd.concat(timetable, ignore_index=True)

        

        dbt_df1= dbTT1.merge( on=['CurveID','AsOfDate','ValueDate','Value'],right=combined_df , indicator=True, how ='outer')

        diff1=dbt_df1.loc[lambda x: x['_merge'] =='right_only']
        diff1=diff1.drop(['_merge'],axis=1)
        diff1= diff1.astype(str).fillna('')
        diff1['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        toDoItems1.set(
        [func.SqlRow(
            {"CurveID": row1['CurveID'],
            "AsOfDate": row1['AsOfDate'],
            "ValueDate": row1['ValueDate'],
            "Value": row1['Value'],
            "Timestamp": row1['Timestamp']
            }
        ) for index, row1 in diff1.iterrows()])

        print(len(diff1))
        t=datetime.now().strftime('%Y-%m-%D %H:%M:%S') 
        send_email(smtp_server, port, sender, password, recipients,subject, f'uploaded {len(diff1)} data input at {t}')
    except Exception as e:
        print(f"Error: {str(e)}")
        send_email(smtp_server, port, sender, password, recipients, subject, str(e))  
        raise  


