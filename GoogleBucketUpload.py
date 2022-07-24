# -*- coding: utf-8 -*-
"""
Created on Fri Jun 17 16:49:44 2022

@author: Nurudeen Ahmed
"""
from google.cloud import storage #Importing Storage module from the Google Cloud Library
import os
import splunklib.client as client #Importing the Splunk Python SDK/Library (The Client module is used for connection and Querying)
import splunklib.results as results #Importing the Splunk Python SDK/Library (The Results module is used for Processing the Query Result)
import pandas as pd #This library will be used to convert the Splunk Result to DataFrame to make it human Readable
from dotenv import load_dotenv  #This library will be used to get the needed Environment Variables
load_dotenv() # This load the library imported in line 13
from datetime import datetime

class GCPUploader: #Creating the Aplication class
    """
        The __init__ is a reserved method in Python. It's known as Constructor and will be called when an object is created from the class and access is required 
        to initialize the attributes of the class.
        I used it here to take care of all the connection(Both Google Cloud and Splunk) needed for the application.
    """
    def __init__(self):
        print("Connecting to Google Console Platform.....")
        self.__key_location_file = os.path.abspath(os.path.join(os.path.dirname(__file__),'key'))
        self.__storage_client = storage.Client.from_service_account_json(self.__key_location_file + '/google-service-key.json')
        print("Connecting to Splunk Localhost Server.....")
        
        """
            The next four lines are used for assigning the Env values to the defined variables(The Variables needed for the Splunk Connection)
        """
        self.__host="localhost"
        self.__port=8089
        self.__username="You Splunk Username"
        self.__password="Splunk Password"
        
        """
            The Next six lines are used for connection to the Splunk Server
        """
        self.__service = client.connect(
                host=self.__host,
                port=self.__port,
                username=self.__username,
                password=self.__password
            )
    def create_dataframe(self): #User defined function to execute the Splunk Query and Result Conversion
        res=[] #A variable to store our result - (Python list)
        
        """
            The next line(48) searches the Splunk using the set Query,
            Edit the first argument of self.__service.jobs.export("search .....",**...) to your taste (I mean your search query)
        """
        rr = results.JSONResultsReader(self.__service.jobs.export("search source= index=_internal earliest=-1h | head 5",**{"output_mode":"json"}))
        
        """
            The for-loop loops through the result and push/appends the results to the already defined python list variable `res` 
        """
        for result in rr:
            if isinstance(result, results.Message):
                print("Creating DataFrame.....")
            elif isinstance(result, dict):
                res.append(result)
        assert rr.is_preview == False #Check if the Result is from a completed search,returns AssertionError if Opposite
        
        df = pd.DataFrame(res) #Convert the search result to DataFrame to become more human Readable
        with open('to_upload.txt','w') as f:
            f.seek(0)
            f.write(df.to_string())
            f.truncate()
        return True
    def upload_toBucket(self, project_name, destination_blob_name): #User defined function to Result to Google Bucket
        """Uploading file to the Bucket."""
        print("Uploading file to the Bucket.....")
        
        """
            Next three lines process the upload to Google Cloud Bucket - into the Blob name specified
        """
        bucket = self.__storage_client.bucket(project_name) 
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename("to_upload.txt",timeout=300)
        print("Upload Success.....")
        
        
if __name__ == '__main__': #Application Entrance
    upload = GCPUploader() # Calling the Application Class
    
    df = upload.create_dataframe() #Calling the defined library meant for Searching the Splunk and converting it to DataFrame
    
    """
        The next two lines set the Environment Variables as specified below
    """
    project_name ="Bucket Name"
    new = datetime.now()
    destination_blob_name=new.strftime("%Y:%m:%d:%H:%M:%S")
    if df is True:
        upload.upload_toBucket(project_name,destination_blob_name)  #Calling the defined library to upload the data into the Cloud
    else:
        print("Unable to Fetch Splunk Data")