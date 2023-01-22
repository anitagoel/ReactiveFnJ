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

s3 = s3fs.S3FileSystem(anon=False)
#{
#  "Records": [
#    {
#      "s3": {
#        "bucket": {
#          "name": "extsorting",
#          "arn": "arn:aws:s3:::extsorting"
#        },
#        "object": {
#          "inFile1" : "input/input1.csv" ,
#          "in1LinePos": 0,
#          "inFile2" : "input/input2.csv" ,
#          "in2LinePos": 0,
#	       "outFile": "outFile/output.csv",
#          "finalMergedFile": "outFile/output.csv",
#	       "mergeThreshold" : 100,
#          "iteration" : 0
#        }
#      }
#    }
#  ]
#}
def lambda_handler(event, context):
    #print("Received event: " + str(event))

    for record in event['Records']:
        # Create some variables that make it easier to work with the data in the
        # event record.
        bucket = record['s3']['bucket']['name']
        arn = record['s3']['bucket']['arn']
        inFile1 =  record['s3']['object']['inFile1']
        in1LinePos = record['s3']['object']['in1LinePos']
        inFile2 = record['s3']['object']['inFile2']
        in2LinePos = record['s3']['object']['in2LinePos']
        outFile = record['s3']['object']['outFile']
        mergedFile = record['s3']['object']['finalMergedFile']
        mergeThreshold = record['s3']['object']['mergeThreshold']
        iteration = record['s3']['object']['iteration']

    #safety against infinite loo
    if iteration < 10:
        print("Current Iteration:",iteration)
        iteration += 1
        input_file1 = os.path.join(bucket, inFile1)
        input_file2 = os.path.join(bucket, inFile2)
        output_file = os.path.join(bucket, outFile)
        finalMergedFile = os.path.join(bucket, mergedFile) 

        fileSize1 = s3.size(input_file1)
        fileSize2 = s3.size(input_file2)
        rowsMerged= 0
        rowFrom1 = ""
        rowFrom2 = ""
        nextThreadInitiated = 0

        with s3.open(input_file1, 'r', newline='') as file1:
            file1.seek(in1LinePos)
            with s3.open(input_file2, 'r', newline='') as file2:
                file2.seek(in2LinePos)
                rowFrom2 = file2.readline()
                in2LinePos += len(rowFrom2)
                print("1 in1LinePos:", in1LinePos, "in2LinePos:", in2LinePos)
                print("Length rowFrom2", len(rowFrom2),"string: rowFrom2:", rowFrom2)
                # Write output file in append mode
                with s3.open(output_file, 'a', newline='') as fileout:
                    # loop thru the first file
                    for rowFrom1 in file1:
                        print("ForLoop: row 1", rowFrom1, "row 2", rowFrom2)
                        print("Length rowFrom1", len(rowFrom1),"string: rowFrom1:", rowFrom1)
                        in1LinePos += len(rowFrom1)
                        print("2 in1LinePos:", in1LinePos, "in2LinePos:", in2LinePos)
                        # check if process just starting and both files are at start position, in that case print header and skip header in second file
                        if iteration == 1:
                            iteration += 1
                            header = rowFrom1
                            fileout.write(header)
                            rowsMerged += 1
                            #print("1st Row, Wrote header in output:", header)
                            # ignore file 2 header
                            if in2LinePos < fileSize2:
                                #print("Skipped Header from new part file:", rowFrom2)
                                rowFrom2 = file2.readline()
                                in2LinePos += len(rowFrom2)
                                print("3 in1LinePos:", in1LinePos, "in2LinePos:", in2LinePos)
                        else:
                            print("For-Else: row 1", rowFrom1, "row 2", rowFrom2)
                            if rowFrom2 < rowFrom1:
                                while (rowFrom2 < rowFrom1 and in2LinePos < fileSize2):
                                    print("For-Else-While: row 1", rowFrom1, "row 2", rowFrom2)
                                    #print("1 wrote part file data:", rowFrom2, " While output file data is", rowFrom1)
                                    fileout.write(rowFrom2)
                                    rowsMerged += 1
                                    if in2LinePos < fileSize2:
                                        rowFrom2 = file2.readline()
                                        in2LinePos += len(rowFrom2)
                                        print("4 in1LinePos:", in1LinePos, "in2LinePos:", in2LinePos)

                                # If last line of file 2 reached, Process it
                                if rowFrom2 < rowFrom1 and in2LinePos == fileSize2:
                                    #print("2 wrote part file data:", rowFrom2, " While output file data is", rowFrom1)
                                    print("For-Else-LastLine File 2- row 1", rowFrom1, "row 2", rowFrom2)
                                    fileout.write(rowFrom2)
                                    rowsMerged += 1
                                    #file 2 complete, increment position so it is not read again
                                    in2LinePos += len(rowFrom2)
                                    print("5 in1LinePos:", in1LinePos, "in2LinePos:", in2LinePos)
                            # end else

                            fileout.write(rowFrom1)

                            rowsMerged += 1
                            #print("3 wrote output file data:", rowFrom1, " While last part file data is", rowFrom2)

                            # check for merge threshold
                            if rowsMerged > mergeThreshold:
                                print("rowsMerged", rowsMerged)
                                in1LinePos -= len(rowFrom1)
                                print("6 reduce in1LinePos:", in1LinePos, "in2LinePos:", in2LinePos)
                                #fileout.close()
                                #file1.close()
                                #file2.close()
                                nextThreadInitiated = 1
                                spanNextInstance(bucket, arn, inFile1, in1LinePos, inFile2, in2LinePos, outFile, mergedFile, mergeThreshold, iteration)
                                break
                    # end for loop

                    # if there are still rows left in file 2 after completion of file 1
                    while rowFrom2 > rowFrom1 and in2LinePos < fileSize2 and rowsMerged < mergeThreshold and nextThreadInitiated == 0:
                        fileout.write(rowFrom2)
                        rowsMerged += 1
                        # check for merge threshold
                        print("End While Loop - row 1", rowFrom1, "row 2", rowFrom2)
                        if rowsMerged > mergeThreshold:
                            #fileout.close()
                            #file1.close()
                            #file2.close()
                            print(" End While Loop - start new lambda")
                            nextThreadInitiated = 1
                            spanNextInstance(bucket, arn, inFile1, in1LinePos, inFile2, in2LinePos, outFile, mergedFile, mergeThreshold, iteration)
                            break
                        #print("4 There is still data in part file, writing in order received :", rowFrom2)
                        rowFrom2 = file2.readline()
                        in2LinePos += len(rowFrom2)

                    # End of while loop, now check for final row in file 2 to write
                    if rowFrom2 > rowFrom1 and in2LinePos == fileSize2 and nextThreadInitiated == 0:
                        fileout.write(rowFrom2)
                        in2LinePos += len(rowFrom2)
                        print("7 in1LinePos:", in1LinePos, "in2LinePos:", in2LinePos)
                        print("There is Final data in part file, writing it :", rowFrom2)

        # End of process, now do cleanup
        if nextThreadInitiated == 0:
            #s3.rm(temp_file1)
            #s3.rm(temp_file2)
            #print("Removed temp output file")
            # move outputfile back to original folder
            s3.mv(output_file, finalMergedFile)

def spanNextInstance(bucket, arn, inFile1, in1LinePos, inFile2, in2LinePos, outFile, mergedFile, mergeThreshold, iteration):
    payload = json.dumps({"Records":[{"s3":{"bucket":{"name":bucket,"arn":arn},"object":{"inFile1" : inFile1, "in1LinePos": in1LinePos, "inFile2" : inFile2, "in2LinePos": in2LinePos, "outFile": outFile, "finalMergedFile":mergedFile, "mergeThreshold" : mergeThreshold, "iteration" : iteration }}}]})
    print("Recursive recursiveMergeWorker Function Getting Invoked with Pauload : ",payload)
    invokeLam = boto3.client("lambda", region_name="us-east-2")
    resp = invokeLam.invoke(FunctionName="recursiveMergeWorker2", InvocationType = "Event", Payload = payload)
    #print("newFileSplit Invoke : Done ")
