#!/usr/bin/env python
# coding: utf-8

# In[ ]:


########### Python Form Recognizer Async Analyze - WITH SYS ARGS #############
# importing necessary libraries 
import img2pdf 
from PIL import Image
import os
import os.path
from os import path
import sys
import json
import time
from requests import get, post
import ntpath
import re
from datetime import datetime

def get_result(source, settings_file, output_path):
    
    #Obtained Date and Time
    t = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    
    try:
        #Parsing key values from Settings JSON
        with open(settings_file) as f:
            settings = json.load(f)
            #Obtaining Log Folder Locaton
            log_folder = str(settings['logs_folder'])
            #Obtaining log level. This can be debug or trace
            log_lev = str(settings['logging_level'])
        
        #Extracting Filename and Extension
        fname_dot_ext = ntpath.basename(source)
        a = len(re.findall("[.]",fname_dot_ext))
    
        #Assigning a non zero value to a
        extension = fname_dot_ext.split('.')[a]
        file_name = fname_dot_ext.replace("." + extension, "")

        #Creating Log File and Writing Logs
        log_file = log_folder + "/" + t + "-" + file_name + ".txt"
        
        #Creating a Log Object
        log_object = open(log_file,"w+")

    except Exception as e:
        #Acitity
        print("ERROR: " + "(INITIALIZATION)Log file creation has failed: \n%s" % str(e))
    
    #Trace-Activity: Log File Creation
    msg = "Logging Initialized Script at " + str(log_folder) + "and Log Level: " + str(log_lev)
    logger(log_file, log_object, log_lev, "INITIALIZATION", msg)

    #Trace-Activity: File Name and Extension
    msg = "Filename Identified: " + file_name + ", Extension Identified: " + extension
    logger(log_file, log_object, log_lev, "EXTENSION", msg)
    
    #Deciding the content_type
    try:
        #Parsing content_type from JSON against specified extension
        content_type = str(settings['content_type'][extension])
    except Exception as e:
        #Capturing Error Message
        content_type = str(e)
    
    #Checking whether extension is invalid. Typical invalid response is 'extention'
    if content_type == "'" + extension + "'":
        api_init = False
        
        #Trace Activity
        msg = "Unsupported Extension" + ": " + extension
        logger(log_file, log_object, "ERROR", "EXTENSION", msg)
    else:
        api_init = True
        #Trace-Activity
        msg = "Content Type is " + str(content_type)
        logger(log_file, log_object, log_lev, "CONTENT TYPE", msg)
    
    #Initiating API Requests for Supported Formats
    if api_init == True:
        #Parsing key values from Settings JSON
        apim_key = settings['apikey']
        model_id = settings['model_info']['model_id']
        post_url_suffix = settings['post_url_suffix']
        endpoint = settings['endpoint']

        #Trace - Activity
        msg = "Successfully Imported Settings from JSON:" + "\n" + "Model ID" + str(model_id) + "\n" + "Endpoint" + str(endpoint)
        logger(log_file, log_object, log_lev, "JSON-SETTINGS", msg)

        # Defining Parameters
        post_url = endpoint + post_url_suffix % model_id
        
        #TRACE-Activity
        msg = "Initiating POST with POST URL: " + post_url
        logger(log_file, log_object, log_lev, "POST-STATUS", msg)

        params = {
            "includeTextDetails": True
        }

        headers = {
            # Request headers
            'Content-Type': content_type,
            'Ocp-Apim-Subscription-Key': apim_key,
        }
        with open(source, "rb") as f:
            data_bytes = f.read()

        try:
            resp = post(url = post_url, data = data_bytes, headers = headers, params = params)
            if resp.status_code != 202:
                #Error - Activity
                msg = "POST analyze failed:\n%s" % json.dumps(resp.json())
                logger(log_file, log_object, "ERROR", "POST-STATUS", msg)
                quit()
            
            #If no errors, operation location will be obtained
            get_url = resp.headers["operation-location"]
            post_success = True
            
            #Trace-Activity
            msg = "POST analyze succeeded:\n%s" % resp.headers + "\n" + "POST Request Successful with Operation Localation: " + get_url
            logger(log_file, log_object, log_lev, "POST-STATUS", msg)

        except Exception as e:
            post_success = False
            
            #Error-Activity
            msg = "POST analyze failed:\n%s" % str(e)
            logger(log_file, log_object, "ERROR", "POST-STATUS", msg)
            quit()

        #Getting values fom JSON
        n_tries = int(settings['api_attempts'])
        wait_sec = int(settings['wait_time'])
        max_wait_sec = int(settings['max_wait_time'])
        
        #TRACE - Activity
        msg = "Successfully Obtained GET API Settings: Number of Tries, Wait TIme, Max Wait Time = " + str(n_tries) + "secs =, " + str(wait_sec) + "secs, "+ str(max_wait_sec) + "secs"
        logger(log_file, log_object, log_lev, "JSON-SETTINGS", msg)

        if post_success == True:
            n_try = 0
            while n_try < n_tries:
                
                #Trace-Activity
                str_try = n_try + 1
                msg = "Attemping to GET with attempt number: " + str(str_try)
                logger(log_file, log_object, log_lev, "GET-STATUS", msg)
                
                try:
                    resp = get(url = get_url, headers = {"Ocp-Apim-Subscription-Key": apim_key})
                    resp_json = resp.json()
                    resp_json
                    #Checking based on the status
                    status = resp_json["status"]
                    
                    #Trace-Activity
                    msg = "Getting Status: " + status
                    logger(log_file, log_object, log_lev, "GET-STATUS", msg)
                    
                    #Chekcing Code
                    if resp.status_code != 200:
                        #Setting Get Flag and Logging
                        get_success = False
                        
                        #Error-Activity
                        msg = "GET analyze results failed:\n%s" % json.dumps(resp_json)
                        logger(log_file, log_object, "ERROR", "GET-STATUS", msg)
                        quit()

                    if status == "succeeded":
                        data_result = resp_json['analyzeResult']
                        #Setting Get Flag and Logging
                        get_success = True
                        
                        #Trace-Activity
                        msg = "Analysis succeeded:\n%s" % json.dumps(data_result) + "\n" + "Exiting From Try Loop"
                        logger(log_file, log_object, log_lev, "GET-STATUS", msg)                   
                        #Exiting Loop
                        break;
                        
                    if status == "failed":
                        #Setting Get Flag
                        get_success = False
                        
                        #Error-Activity
                        msg = "Analysis failed:\n%s" % json.dumps(resp_json) + "\n" + "Quitting from Script"
                        logger(log_file, log_object, "ERROR", "GET-STATUS", msg)
                        quit()
                    
                    # Analysis still running. Wait and retry.
                    time.sleep(wait_sec)
                    n_try += 1
                    wait_sec = min(2*wait_sec, max_wait_sec)
                    
                    #Trace-Activity
                    msg = "Waiting for response: " + str(wait_sec) + " secs"
                    logger(log_file, log_object, log_lev, "GET-STATUS", msg)
                    
                except Exception as e:
                    get_success = False
                    
                    #ERROR-Activity
                    msg = "GET analyze results failed:\n%s" % str(e) + "\n" + "Quitting Script Now"
                    logger(log_file, log_object, "ERROR", "GET-STATUS", msg)
                    quit()
                    
                #ERROR-Activity
                msg_e = "Analyze operation did not complete within the allocated time in attempt: " + str(n_try)
                logger(log_file, log_object, "ERROR", "GET-STATUS", msg_e)

            
            if get_success == True:
                
                #Trace-Activity
                msg = "Starting JSON Parsing"
                logger(log_file, log_object, log_lev, "JSON-PARSE", msg)
                
                #JSON PARSING FOR VALUES AS PER CONFIGURED FIELDS IN THE CONFIG FILE
                #Importing Field Values from Settings File
                field_array = settings['model_info']['fields_scope']
                field_len = len(field_array)
                
                #Trace-Activity
                msg = str(field_len) + " fields found for scope of parsing from settings file"
                logger(log_file, log_object, log_lev, "JSON-PARSE", msg)

                #Initilizing inal Text Output
                txt_output = ""

                #Looping through Field Array and Subsequently Parsing JSON
                for i in range(field_len):
                    field_in_scope = field_array[i]
                    try:
                        #Parsing JSON with the field in scope
                        field_json = data_result['documentResults'][0]['fields'][field_in_scope]
                        
                        #Checking if field is Null or Not
                        if str(field_json) == "None":
                            field_text = ""
                            
                            #Trace-Activity
                            msg = "Null value for " + field_in_scope
                            logger(log_file, log_object, log_lev, "JSON-PARSE", msg)
                        else:
                            field_text = field_json['text']
                            
                            #Trace-Activity
                            msg = field_in_scope + " data succeasfully parsed"
                            logger(log_file, log_object, log_lev, "JSON-PARSE", msg)

                        #Appending to Text Output
                        line_item = "Value" + field_in_scope + " : " + field_text
                        txt_output = txt_output + line_item + "\n"
                        
                    except Exception as e:
                        #Error-Activity
                        parsemsg = "JSON Parsing Failed:\n%s" % str(e)
                        logger(log_file, log_object, "ERROR", "JSON-PARSE", parsemsg)
                
                try:
                    #Creating Output Text File
                    txt_file = output_path + "/" + file_name + ".txt"
                    file_object = open(txt_file,"w+")

                    # the result is a Python dictionary:
                    file_object.write(txt_output)
                    file_object.close()
                    
                    #Setting Flag as True
                    txt_flag = True
                    
                    #Trace-Activity
                    msg = "Successfully created text file and logged data in: " + txt_file
                    logger(log_file, log_object, log_lev, "TXT-OUTPUT", msg)
                
                except Exception as e:
                    msg = str(e)
                    
                    #Setting FLag as False
                    txt_flag = False
                    
                    #ERROR-Activity
                    msg = "Text File Creation Failed" + msg + "\n" + "Quitting Script Now"
                    logger(log_file, log_object, "ERROR", "TXT-OUTPUT", msg)
                    quit()
                
                
                if txt_flag == True:  
                    #Reading JSON Setting for conversion
                    pdf_flag = settings['convert_to_pdf']
                    delete_flag = settings['delete_original_files']

                    #Trace-Activity
                    msg = "Reading PDF Convert Setting: " + pdf_flag + "and Delete Flag: " + delete_flag
                    logger(log_file, log_object, log_lev, "JSON-SETTINGS", msg)
                    
                    #Converting Original Image to PDF for downstream
                    if pdf_flag == "Yes":

                        # storing pdf path
                        pdf_path = os.path.dirname(source) + "/" + file_name + ".pdf"

                        # opening image 
                        image = Image.open(source) 

                        # converting into chunks using img2pdf 
                        pdf_bytes = img2pdf.convert(image.filename)

                        #Trace-Activity
                        msg = "Images will be converted to pdf with output path" + str(pdf_path)
                        logger(log_file, log_object, log_lev, "PDF-CONVERT", msg)

                        # opening or creating pdf file 
                        file = open(pdf_path, "wb") 
                        log_object.write("PDF File Created")

                        # writing pdf files with chunks 
                        file.write(pdf_bytes) 

                        # Closing image file 
                        image.close()

                        #Deleting Original File Based on FLag
                        if delete_flag == "Yes":
                            os.remove(source)  
                            #Trace-Activity
                            msg = "Successfully Deleted File: " + source
                            logger(log_file, log_object, log_lev, "PDF-CONVERT", msg)

                        # closing pdf file 
                        file.close()

                        # Trace-Acitivity
                        msg = "Successfully Built PDF File in Path:" + pdf_path
                        logger(log_file, log_object, log_lev, "PDF-CONVERT", msg)
                
                        #Closing Log File Session
                        log_object.close()
 
#Defining Logger Function
def logger(log_file, log_object, log_lev, module, log_msg):
    try:
        #Opening Log File
        if path.exists(log_file):
            log_t = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
            if log_lev == "TRACE":
                print("TRACE: " + "(" + module + ")" + log_msg + "\n")
                log_object.write(log_t + ": TRACE: " + "(" + module + ")" + log_msg + "\n")
            elif log_lev == "ERROR":
                print("ERROR: " + "(" + module + ")" + log_msg + "\n")
                log_object.write("ERROR: " + "(" + module + ")" + log_t + log_msg + "\n")
        else:
            print("Log File doesn't exists")
    except Exception as e:
        print("Invalid Logging Level" + "\n" + str(e))

#Orchestration on CMD
if __name__ == "__main__":
   get_result(*sys.argv[1:])    

