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

#processed_folder = "processed"
#merged_folder = "merged"
#output_folder = "output"
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
        key = record['s3']['object']['key']

    ms3 = boto3.resource('s3')
    mybucket = ms3.Bucket(bucket)

    # check if there already another file to merge
        # Start the function that processes the incoming data.
    input_file = os.path.join(bucket, key)
    temp_path = os.path.join(bucket, temp_folder)
    #print(temp_path)
    # filesize excluding the header
    in_file_size = file_size(input_file)
    #in_file_size = 10000

    objs = list(mybucket.objects.filter(Prefix='Parallel/File'))
    for w in objs :
        file2 = os.path.join(bucket, w.key) 
        if file2 != input_file :
            #print(w.key)
            print("another file found, now we would merge input file in it, File1:", input_file,"File2:", file2)
            process_incoming_file(input_file, file2, temp_path, in_file_size,bucket)
            process_status = 'done'
            break
    #file2 = os.path.join(bucket, "Parallel/File2.csv") 
    #process_incoming_file(input_file, file2, temp_path, in_file_size)
    # check if there are any more files to merge
    # input_dir = os.path.dirname(input_file)
    # print("input Dir: ",input_dir)

##    if process_status == "done":
##        #print("now checking for more files")
##        obs = list(mybucket.objects.filter(Prefix="processed/testfile"))
##        for obj_sum in obs:
##            #print(obj_sum.key)
##            # call this Lambda again from here
##            invokeLam = boto3.client("lambda", region_name="us-east-2")
##            # payload = {"message": "Test"}
##            # payload = "{\"Records\": [{ \"s3\": {\"bucket\": {\"name\": \"extsorting\", \"arn\": \"arn:aws:s3:::extsorting\"},\"object\": {\"key\": \""+ obj_sum.key +"\"}}}]}"
##            payload = json.dumps({"Records": [{"s3": {
##                "bucket": {"name": "extsorting", "arn": "arn:aws:s3:::extsorting"}, "object": {"key": obj_sum.key}}}]})
##            #print("CustomerImport_Reducer Getting Invoked with Pauload : ", payload)
##            resp = invokeLam.invoke(FunctionName="CustomerImport_Reducer", InvocationType="Event", Payload=payload)
##            #print("CustomerImport_Reducer Invoked ")
##            break

# Determine the number of files that this Lambda function will create.
def file_size(input_file):
    import csv
    file_handler=s3.open(input_file, 'r')
    reader = csv.reader(file_handler, delimiter=',')
    # Figure out the number of rows in the file
    row_count = sum(1 for row in reader) - 1
    return row_count
    
# process the input data
# write to output files.
def process_incoming_file(input_file, input_file1, temp_path, in_file_size,bucket):
    # Move input file and outputfile to a teamp location.
    #print(input_file, input_file1, temp_path, in_file_size)
    temp_file_name1 = os.path.splitext(os.path.basename(input_file1))[0] + "_1.csv"
    temp_file_name = os.path.splitext(os.path.basename(input_file))[0] + "_2.csv"
    merged_file_name = os.path.splitext(os.path.basename(input_file1))[0] + "_" + os.path.splitext(os.path.basename(input_file))[0] + ".csv"
    temp_file1 = os.path.join(temp_path, temp_file_name1)
    temp_file2 = os.path.join(temp_path, temp_file_name)
    
    merged_file = os.path.join(bucket , "Parallel", merged_file_name)
    
    out_file_name = os.path.splitext(os.path.basename(input_file))[0] + "_12.csv"
    output_file = os.path.join(temp_path, out_file_name)
    cur_infile_row = 0
    #print("inside the process incoming file fucntion")
    s3.mv(input_file1, temp_file1)
    s3.mv(input_file, temp_file2)

    # Now open both temp files and merge into the output file
    header = ''
    linecount = 0

    with s3.open(temp_file1, 'r', newline='', encoding='utf-8-sig') as outFile:
        with s3.open(temp_file2, 'r', newline='', encoding='utf-8-sig') as partFile:
            partRow = partFile.readline()
            cur_infile_row += 1
            # Write output file
            with s3.open(output_file, 'w', newline='', encoding='utf-8-sig') as fileout:
                # loop thru the output file
                for outRow in outFile:
                    linecount += 1
                    if linecount == 1:
                        header = outRow
                        fileout.write(header)
                        #print("1st Row, Wrote header in output:", header)
                        # ignore file 2 header
                        if cur_infile_row < in_file_size:
                            #print("Skipped Header from new part file:", partRow)
                            partRow = partFile.readline()
                            cur_infile_row += 1
                    else:
                        # print("0 row2:",row2, " row:",outRow)
                        if partRow < outRow:
                            while (partRow < outRow and cur_infile_row < in_file_size):
                                #print("1 wrote part file data:", partRow, " While output file data is", outRow)
                                fileout.write(partRow)
                                if cur_infile_row < in_file_size:
                                    partRow = partFile.readline()
                                    cur_infile_row += 1
                            if partRow < outRow and cur_infile_row == in_file_size:
                                #print("2 wrote part file data:", partRow, " While output file data is", outRow)
                                fileout.write(partRow)
                                cur_infile_row += 1
                        fileout.write(outRow)
                        #print("3 wrote output file data:", outRow, " While last part file data is", partRow)
 
                # if there are still rows left in file 2
                while partRow > outRow and cur_infile_row < in_file_size:
                    fileout.write(partRow)
                    #print("4 There is still data in part file, writing in order received :", partRow)
                    partRow = partFile.readline()
                    cur_infile_row += 1
                if partRow > outRow and cur_infile_row == in_file_size:
                    fileout.write(partRow)
                    cur_infile_row += 1
                    #print("There is Final data in part file, writing it :", partRow)
 
    # remove temp files
    s3.rm(temp_file1)
    s3.rm(temp_file2)
    #print("Removed temp output file")
    # move outputfile back to original folder
    s3.mv(output_file, merged_file)
    
