#cloud_function(platforms=[Platform.AWS], memory=512, config=config)
def yourFunction(request, context):
    import boto3
    import pandas
    import io
    from Inspector import Inspector

    #pass lambda function arguments
    s3_bucket = request['s3_bucket']
    s3_infile = request['s3_infile']
    s3_outfile = request['s3_outfile']
    result = {}
    
    #fetching s3 object into pandas dataframe
    s3 = boto3.client('s3')
    file_obj = s3.get_object(Bucket = s3_bucket, Key = s3_infile)
    data = pandas.read_csv(file_obj['Body'])
    
    #stats of the data frame object "data"
    result["Data info"] = data.info()
    print(result["Data info"])
    
    #Formatting columns
    data["Order Date"] = pandas.to_datetime(data["Order Date"])
    data["Ship Date"] = pandas.to_datetime(data["Ship Date"])
    data.loc[data["Order Priority"] == "H", "Order Priority"] = "High"
    data.loc[data["Order Priority"] == "L", "Order Priority"] = "Low"
    data.loc[data["Order Priority"] == "C", "Order Priority"] = "Critical"
    data.loc[data["Order Priority"] == "M", "Order Priority"] = "Medium"
    
    print("Deriving new columns from existing columns")
    #Deriving new columns from existsing columns
    data["Processing time"] = data["Ship Date"] - data["Order Date"] 
    data["Gross Margin"] = (data["Total Profit"] / data["Total Revenue"]) * 100
    
    print("Transformation done... now uploading")
    print(data.info())
    csv_buffer= io.StringIO()
    data.to_csv(csv_buffer)
    s3_resource_obj = boto3.resource('s3')
    s3_resource_obj.Object(s3_bucket, s3_outfile).put(Body=csv_buffer.getvalue())
    print("file Uploaded successfully ...")

    result["message"] = "Successfully uploaded data into s3"
    return result
