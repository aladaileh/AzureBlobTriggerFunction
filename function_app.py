import azure.functions as func
import logging
from azure.functions.decorators.core import DataType
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
from io import StringIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def get_db_connection():
    server = 'ahmad1997.database.windows.net'
    database = 'scrapeddata'
    username = 'ahmadriad'
    password = 'Ara12345'
    driver = '{ODBC Driver 17 for SQL Server}'
    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    return pyodbc.connect(connection_string)


app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="gasconsumption/inputs_df.csv",
                               connection="AzureWebJobsStorage1") 
@app.blob_input(arg_name="inputblob", path="gasconsumption/inputs_df.csv",
                               connection="AzureWebJobsStorage1")
@app.blob_input(arg_name="inputblob1", path="gasconsumption/AemoNomsAndForcastFlow.csv",
                               connection="AzureWebJobsStorage1")
@app.blob_input(arg_name="inputblob2", path="gasconsumption/KHNPAvialability.csv",
                               connection="AzureWebJobsStorage1")
@app.generic_output_binding(arg_name="toDoItems", type="sql", CommandText="dbo.DefTable", ConnectionStringSetting="SqlConnectionString",data_type=DataType.STRING)


def DefinitionTable(myblob: func.InputStream, inputblob: str, inputblob1: str,inputblob2: str,  toDoItems: func.Out[func.SqlRow]) :
    
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
    subject="Error in Code (AemoNomsAndForecastFlow)"
    
    
    
    try:
    # data = inputblob
    # df = pd.read_csv(StringIO(data))
        
        
        conn = get_db_connection()
        cursor = conn.cursor()
        rows= cursor.execute("SELECT * FROM dbo.DefTable").fetchall()
        existing_ids=pd.DataFrame(rows)
        column_names=['CurveID','Location','Commodity','Period','Source','Senario','Type','SubType','SubSubTybe','Unit','HubType','FreeText','Timestamp']
        e=[list(d) for d in existing_ids[0]]

        dbc= pd.DataFrame(e, columns=column_names)
        dbt=dbc.drop(['CurveID','Timestamp'], axis=1).fillna('')

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
    except Exception as e:
        raise Exception(send_email(smtp_server, port, sender, password, recipients,subject, str(e)))


@app.blob_trigger(arg_name="myblob", path="gasconsumption/inputs_df.csv",
                               connection="AzureWebJobsStorage1") 
@app.blob_input(arg_name="inputblob0", path="gasconsumption/inputs_df.csv",
                               connection="AzureWebJobsStorage1")
@app.blob_input(arg_name="inputblob01", path="gasconsumption/AemoNomsAndForcastFlow.csv",
                               connection="AzureWebJobsStorage1")
@app.blob_input(arg_name="inputblob02", path="gasconsumption/KHNPAvialability.csv",
                               connection="AzureWebJobsStorage1")
@app.generic_output_binding(arg_name="toDoItems1", type="sql", CommandText="dbo.TimeSeries", ConnectionStringSetting="SqlConnectionString",data_type=DataType.STRING)
def TimeSeriesTable(myblob: func.InputStream, inputblob0: str, inputblob01: str, inputblob02: str, toDoItems1: func.Out[func.SqlRow]) :
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
    subject="Error in Code (TimeSeriesTable)"

    try:
            
                
        conn1 = get_db_connection()
        cursor1 = conn1.cursor()
        rows1= cursor1.execute("SELECT * FROM dbo.DefTable").fetchall()
        existing_ids1=pd.DataFrame(rows1)
        column_names1=['CurveID','Location','Commodity','Period','Source','Senario','Type','SubType','SubSubTybe','Unit','HubType','FreeText','Timestamp']
        i=[list(g) for g in existing_ids1[0]]

        dbDT1= pd.DataFrame(i, columns=column_names1).fillna('')


        rows2= cursor1.execute("SELECT * FROM dbo.TimeSeries").fetchall()
        existing_ids2=pd.DataFrame(rows2)
        column_names2=['CurveID','ValueDate','AsOfDate','Value','Timestamp']
        k=[list(d) for d in existing_ids2[0]]

        dbt1= pd.DataFrame(k, columns=column_names2)
        dbTT1=dbt1.drop(['Timestamp'],axis=1).fillna('')

         
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
        # counter = 0
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
            # print(v ,y)
            # print(x,z)
            # counter +=1
            # print(counter)
            if v.equals(x) and y.equals(z):
                print('All good')
            else:
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
        raise Exception(send_email(smtp_server, port, sender, password, recipients,subject, str(e)))

