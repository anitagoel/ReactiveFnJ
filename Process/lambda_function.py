import os
import csv
import boto3
import s3fs
from botocore.exceptions import ClientError
#import process_input as inputProcessor

processed_folder = "processed"
s3 = s3fs.S3FileSystem(anon=False)

#{   "Records": [
#    {
#      "s3": {
#        "bucket": {
#          "name": "bucket-name",
#          "arn": "arn:aws:s3:::bucket-name"
#        },
#        "object": {
#          "key": "inputFull/testfile.csv"
#        }      }    }  ]}

def lambda_handler(event, context):
    print("Received event: " + str(event))

    for record in event['Records']:
        # Create some variables that make it easier to work with the data in the
        # event record.
        bucket = record['s3']['bucket']['name'] # bucket = extsorting
        key = record['s3']['object']['key'] # key = inputFull/testfile.csv
        input_file = os.path.join(bucket, key) # input_file = extsorting/inputFull/testfile.csv 
        # os.path.basename(input_file) will return testfile__part2__of5.csv
        # os.path.splitext(os.path.basename(input_file))[0] will return extsorting/inputFull/testfile
        # output_file_name = extsorting/inputFull/testfile.csv
        output_file_name = os.path.splitext(os.path.basename(input_file))[0] + "_processed.csv"
        processed_subfolder = os.path.basename(input_file).split("__part", 1)[0]
        output_fullpath = os.path.join(bucket, processed_folder, output_file_name)
        #output_fullpath = os.path.join(bucket, processed_folder, processed_subfolder, output_file_name)
        # Start the function that processes the incoming data.
        process_incoming_file(input_file, output_fullpath)


# process the input data
# write to output files.
def process_incoming_file(input_file, output_file):
    # Counters for tracking the number of records and endpoints processed.
    header = ''
    linecount = 0
    dataList = list()

    folder = os.path.split(output_file)[0]

    with s3.open(input_file, 'r', newline='', encoding='utf-8-sig') as inFile:
        #fileReader = csv.reader(inFile)

        for row in inFile:
            # Sleep to prevent throttling errors.
            #time.sleep(.025)
            # save the header row.
            linecount += 1
            if linecount == 1:
                header = row
            else:
                dataList.append(row.strip())
   

    #sort data List
    dataList.sort()

    # Write output file
    with s3.open(output_file, 'w', newline='', encoding='utf-8-sig') as fileout:
        #fileWriter = csv.writer(outFile)
        outline = 0
        #with open(filename, 'w') as fileout :
        for data in dataList :
            outline += 1
            if outline == 1 :
                fileout.write(header )
            fileout.write( data +'\n' )

    s3.rm(input_file)