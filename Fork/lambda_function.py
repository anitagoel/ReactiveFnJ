import os
import json
import s3fs
import boto3

#{
#  "Records": [
#    {
#      "s3": {
#        "bucket": {
#          "name": "extsorting",
#          "arn": "arn:aws:s3:::extsorting"
#        },
#        "object": {
#          "dirPath" : "input1" ,
#          "fileName": "input/input.txt",
#	       "startByte": 1,
#	       "readSize" : #as per runtime environment#,
#	       "searchChar" : '\n',
#	       "fileNum" : 1
#        }
#      }
#    }
#  ]
#}

s3 = s3fs.S3FileSystem(anon=False)

def lambda_handler(event, context):
    #print("Received event: " + str(event))

    for record in event['Records']:
        # Create some variables that make it easier to work with the data in the
        # event record.

        bucket = record['s3']['bucket']['name']
        dirPath =  record['s3']['object']['dirPath']
        fileName = record['s3']['object']['fileName']
        startByte = record['s3']['object']['startByte']
        readSize = record['s3']['object']['readSize']
        searchChar = record['s3']['object']['searchChar']
        fileNum = record['s3']['object']['fileNum']
        bytesRead = ""

        input_file = os.path.join(bucket, dirPath, fileName)

        fileSize = s3.size(input_file)
        #print("Total Size: ", fileSize)

        with s3.open(input_file, 'r') as f:
            # find the next newline
            f.seek(startByte+readSize) # seek is initially at byte 0 and then moves forward the specified amount, so seek(5) points at the 6th byte
            bytesRead = f.read(1)
            addCount = 0
            if bytesRead != searchChar:
                while bytesRead != searchChar and (startByte + readSize + addCount) < fileSize:
                    #print("byteread:",bytesRead,":")
                    addCount = addCount + 1
                    f.seek(startByte + readSize + addCount)
                    bytesRead = f.read(1)
            #print(" addCount :", addCount,", Final Read till:",startByte+readSize+addCount+1)
            f.seek(startByte-1) # seek is initially at byte 0 and then moves forward the specified amount, so seek(5) points at the 6th byte
            bytesRead = f.read( readSize + addCount )
            #print("Return Bytes: ", bytesRead)
            #print("-----------------------")
        # now right these in a new file
        newFile = str(fileNum) +  fileName
        newFilePath = os.path.join(bucket, dirPath, newFile)
        #newFile = str(fileNum)+ fileName
        fw = s3.open(newFilePath, "w")
        fw.write(bytesRead)
        fw.close()

        #check if there is still data in file
        if (startByte + readSize + addCount) < fileSize:
            newStartByte = startByte + readSize + addCount
            fileNum = fileNum + 1
            # Invoke Lambda recursively
            invokeLam = boto3.client("lambda", region_name="us-east-2")
            #payload = {"message": "Test"}
            #payload = "{\"Records\": [{ \"s3\": {\"bucket\": {\"name\": \"extsorting\", \"arn\": \"arn:aws:s3:::extsorting\"},\"object\": {\"key\": \""+ obj_sum.key +"\"}}}]}"
            payload = json.dumps({"Records":[{"s3":{"bucket":{"name":"extsorting","arn":"arn:aws:s3:::extsorting"},"object":{"dirPath": dirPath, "fileName": fileName, "startByte": newStartByte, "readSize" : readSize, "searchChar" : searchChar, "fileNum" : fileNum }}}]})
            #print("Recursive FileSplit Function Getting Invoked with payload : ",payload)
            resp = invokeLam.invoke(FunctionName="FileSplit", InvocationType = "Event", Payload = payload)
            #print("FileSplit Invoke : Done ")
            # ReadFileSegment((startByte + readSize + addCount+3), readSize, fileName, fileBytes, searchChar, fileNum)
