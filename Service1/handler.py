from Inspector import Inspector
import time
import boto3
from datetime import datetime
import csv
import json
import logging


def lambda_handler(request, context):
   
    # extract message and key from input json
    bucket = request['bucketname']
    key = request['key']
   
    # connect to boto3
    s3 = boto3.client('s3')
   
    # download the s3 sales data as download.csv.
    new_file_name = '/tmp/download.csv'
    s3.download_file(bucket, key, new_file_name)
   
    print("Reading CSV")
    # lists to hold transformations.
    Gross_Margin_list = []
    processing_times_list = []
    # variable used to process header
    line_count = 0
   
    with open(new_file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            # ignore the header
            if line_count == 0:
                pass
                #print(f'Column names are {", ".join(row)}')
            else:
                # calculate percentage profit
                Gross_Margin = (float(row[13])/float(row[12])) * 100
                Gross_Margin_list.append(Gross_Margin)
               
                #calculate order processing time
                date_format = "%m/%d/%Y"
                order_date = datetime.strptime(row[5], date_format)
                ship_date = datetime.strptime(row[7], date_format)
                processing_time = ship_date - order_date
                processing_times_list.append(processing_time.days)
               
               
            line_count += 1
     
        print(f'Processed {line_count} lines.')

    orderid = []
    i = 0
    with open(new_file_name,'r') as infile:
        reader = list(csv.reader(infile))
        # append the new columns to the original header
        reader[0].append("Gross_Margin")
        reader[0].append("Order_Processing_days")
        # write to the download.csv output file
        with open(new_file_name, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(reader[0])
            for row in reader[1:]:
                # pop from the transformation lists and append it to row.
                Gross_Margin = Gross_Margin_list.pop(0)
                processing_time = processing_times_list.pop(0)
                row.append(Gross_Margin)
                row.append(processing_time)
                #print(type(row[4]))
                row[4] = ConvertOrderPriority(row[4])
                # write the row to the download.csv file
                if row[6] not in orderid:
                    writer.writerow(row)
                    orderid.append(row[6])
                   

    # finally upload the transformed file to s3.        
    s3.upload_file(new_file_name, bucket, "101_sales_data_transformed.csv")
    print(i)

       
    # Call inspect all function and pass the message and key
    inspector = Inspector()
   # inspector.inspectAll()
   
 


    return inspector.finish()

def ConvertOrderPriority(argument):
    switcher = {
        "H" : "High",
        "L" : "Low",
        "M" : "Medium",
        "C" : "Critical"
       
    }
    return switcher[argument]