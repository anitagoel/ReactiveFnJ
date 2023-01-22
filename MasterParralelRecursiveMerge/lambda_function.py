import io
import os
import csv
import time
import uuid
import boto3
import botocore
import s3fs
import json
from botocore.exceptions import ClientError

# import process_input as inputProcessor

temp_folder = "temp"
s3 = s3fs.S3FileSystem(anon=False)
process_status = "notdone"


# {   "Records": [
#    {
#      "s3": {
#        "bucket": {
#          "name": "extsorting",
#          "arn": "arn:aws:s3:::extsorting"
#        },
#        "object": {
#          "key": "Parallel/File1.csv"
#        }      }    }  ]}

def lambda_handler(event, context):
    #print("Received event: " + str(event))

    for record in event['Records']:
        # Create some variables that make it easier to work with the data in the
        # event record.

        bucket = record['s3']['bucket']['name']
        arn = record['s3']['bucket']['arn']
        key = record['s3']['object']['key']
        filePrefix='input/in'
        #"Parallel/File"

    ms3 = boto3.resource('s3')
    mybucket = ms3.Bucket(bucket)

    # Start the function that processes the incoming data.
    inputFile1 = os.path.join(bucket, key)

    # check if there already another file to merge
    print("starting this process")
    objs = list(mybucket.objects.filter(Prefix=filePrefix))
    for w in objs :
        input_file2 = os.path.join(bucket, w.key)
        if input_file2  != inputFile1 :
            print("another file found, now we would merge input file in it, File1:", inputFile1,"File2:", input_file2 ,"temp_folder:", temp_folder," bucket:", bucket)
            process_incoming_file(inputFile1, input_file2, temp_folder, bucket, arn)
            process_status = 'done'
            break
    print("cloing this process")

# process the input data
# write to output files.
def process_incoming_file(inputFile1, inputFile2, temp_path, bucket, arn):
    # Move input file and outputfile to a teamp location.
    print(" inside the  process_incoming_file")
    temp_file_name1 = os.path.splitext(os.path.basename(inputFile1))[0] + "_2.csv"
    temp_file_name2 = os.path.splitext(os.path.basename(inputFile2))[0] + "_1.csv"
    out_file_name = os.path.splitext(os.path.basename(inputFile1))[0] + "_12.csv"
    merged_file_name = os.path.splitext(os.path.basename(inputFile2))[0] + "_" + os.path.splitext(os.path.basename(inputFile1))[0] + ".csv"

    temp_file1 = os.path.join(temp_path, temp_file_name1)
    temp_file2 = os.path.join(temp_path, temp_file_name2)
    output_file = os.path.join(temp_path, out_file_name)
    final_merged_file = os.path.join("Parallel", merged_file_name)

    temp1 = os.path.join(bucket, temp_file1)
    temp2 = os.path.join(bucket, temp_file2)

    print(" Moving inputFile1:", inputFile1, "  to temp_file1:", temp1)
    s3.mv(inputFile1, temp1)
    print(" Moving inputFile1:", inputFile2, "  to temp_file1:", temp2)
    s3.mv(inputFile2, temp2)

    payload = json.dumps({"Records":[{"s3":{"bucket":{"name":bucket,"arn":arn},"object":{"inFile1" : temp_file1, "in1LinePos": 0, "inFile2" : temp_file2, "in2LinePos": 0, "outFile": output_file, "finalMergedFile": final_merged_file, "mergeThreshold" : 10, "iteration" : 0 }}}]})
    print("Recursive recursiveMergeWorker Function Getting Invoked with Payload : ",payload)
    invokeLam = boto3.client("lambda", region_name="us-east-2")
    resp = invokeLam.invoke(FunctionName="recursiveMergeWorker", InvocationType = "Event", Payload = payload)
