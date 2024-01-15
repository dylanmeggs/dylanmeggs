import sys
import pandas as pd
import re
import boto3
import csv
import io
from io import StringIO
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions

## SET UP
args = getResolvedOptions(sys.argv,
                          ['REGION','REPORTNAME','BUCKETNAME','PREFIX','WRITEBUCKET','WRITEPATH'])

print(args)                          
## ASSIGN CONFIG PARAMETERS
REGION = args['REGION']
REPORTNAME = args['REPORTNAME']
BUCKETNAME = args['BUCKETNAME']
PREFIX = args['PREFIX']
WRITEBUCKET = args['WRITEBUCKET']
WRITEPATH = args['WRITEPATH']

# other config 
explanatory_note='''

Metric Name: Metric Definition
Metric Name: Metric Definition
Metric Name: Metric Definition
Metric Name: Metric Definition
Etc...
Etc...
Etc...''' 

def get_most_recent_s3_object(bucket_name, prefix):
    '''Open the most recent file in bucket path'''
    s3 = boto3.client('s3')
    paginator = s3.get_paginator( "list_objects_v2" )
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
    return latest

def s3_read_object(s3_client, bucket, key):
    '''Read csv input file'''
    file_obj = s3_client.get_object(Bucket=bucket, Key=key)
    file_content = file_obj["Body"].read()
    read_data = io.BytesIO(file_content)
    df = pd.read_csv(read_data, sep='\t', encoding='utf-8')
    return df

def s3_write_files(s3_client, outdf, bucket, key, excel=False):
    '''Write pandas dataframe to file, either csv or excel'''

    # add a report title as the first line
    # sql output specifies values for each field
    outdf.rename(columns={'col1':'XYZ Report Name Here','col2':'','col3':'','col4':'','col5':'','col6':'','col7':'','col8':'','col9':'','col10':'','col11':'','col12':'','col13':''},inplace=True)
    
    # talk to dmeggs if wanting Excel instead of csv
    if excel:
        with io.BytesIO() as output:
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                outdf.to_excel(writer, startrow = 0, sheet_name='Sheet1', index=False)
    
                workbook = writer.book
                worksheet = writer.sheets['Sheet1']
                
                for i, col in enumerate(outdf.columns):
                    lencol = outdf[outdf.row<1000,col].astype(str).str.len().max()
                    lencol = lencol + 2
                    worksheet.set_column(i, i, lencol)
                
                writer.save()
                data = output.getvalue()
    else:
        csv_buffer = StringIO()
        outdf.to_csv(csv_buffer, index=False)
        data = csv_buffer.getvalue()
    # Destination: https://s3.console.aws.amazon.com/s3/buckets/name-here?region=us-east-1&prefix=xyz/program/input_files/&showversions=false
    response = s3_client.put_object(Bucket=bucket, Key=key, Body=data, GrantFullControl="id=1abcd23456xyz")

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    return status

def s3_write_done_file(s3_client, write_bucket, write_path, week, files, poc='dylan@dylanmeggs.com'):
    
    d = {'week':[week],'total_files':[files],'poc':[poc]}
    donedf = pd.DataFrame(d)
    csv_buffer = StringIO()
    donedf.to_csv(csv_buffer, index=False)
    data = csv_buffer.getvalue()
    # create a file name for the done file
    write_key = write_path+'dsp_pps_done_file.csv'
    response = s3_client.put_object(Bucket=write_bucket, Key=write_key, Body=data, GrantFullControl="id=1abcd23456xyz")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    return status

def main(report_name, bucket_name, prefix, region, write_bucket, write_path, excel=False):
    '''split conslidated csv report file into individual reports'''
    
    # Get file path for most recent added consolidated report
    latest = get_most_recent_s3_object(bucket_name, prefix)
    file_path = latest['Key']
    print(file_path)
    
    # Create s3 client
    s3_client = boto3.client(service_name='s3', region_name=region)
    
    # Load consolidated report into dataframe
    df = s3_read_object(s3_client, bucket_name, file_path)

    # Create dataframe of the unique combinations of report levels (country, station, dsp, date)
    categories = df.groupby(['dsp','station','year','week','country','load_date']).count().reset_index()
    
    file_count = 0
    upload_week = 0


    # iterate over combinations to extract out individual reports
    for i in range(len(categories)):
        

        # filter to specific value
        row = categories.iloc[i]
        print('row:',i)
        country, station, dsp, year, week, load_date = row.country, row.station, row.dsp, row.year, row.week, row.load_date
        filter = (df.country == country) & (df.station == station) & (df.dsp == dsp) & (df.year == year) & (df.week == week) & (df.load_date == load_date)
        reportdf = df.loc[filter].copy()
        reportdf.sort_values(by='row_no', ascending=True, inplace=True)
        # add constant explanatory note at the end of the report, each line of the note should be a row in the table for legibility
        for line in explanatory_note.split('\n'):
            newrow = [dsp, week, year, station, country, load_date, line, '', '', '', '', '', '', '', '', '', '', '', '', 1000]
            reportdf.loc[len(reportdf.index)] = newrow

         # create unique file name
        filename = "{fcountry}-{fdsp}-{fds}-{fweek}-{fyear}-{fjobname}".format(
                                                                                        fcountry=country.upper(),
                                                                                        fdsp=dsp.upper(),
                                                                                        fweek=week,
                                                                                        fyear=year,
                                                                                        fds=station.upper(),
                                                                                        fjobname=report_name
                                                                                    )

        write_key = write_path+filename
        
        if excel: write_key+='.xlsx'
        else: write_key+='.csv'
        
        print('attempt to write file')
        status = s3_write_files(s3_client, reportdf.loc[:,['col1','col2','col3','col4','col5','col6','col7','col8','col9','col10','col11','col12','col13']], write_bucket, write_key, excel=excel)
        print(status)
        
        if status == 200: file_count+=1
        
        
    # write the trigger file
    status = s3_write_done_file(s3_client, write_bucket, write_path, upload_week, file_count)
    if status != 200:
        raise Exception("Done file write failed")
    print(status)
    print('Complete')
    print('Created {} new files'.format(file_count))
        
    
# Parameters set in Glue UI under Job details -> Advanced properties
main(report_name = REPORTNAME, bucket_name = BUCKETNAME,prefix = PREFIX, region = REGION, write_bucket=WRITEBUCKET, write_path=WRITEPATH)   