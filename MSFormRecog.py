#!/usr/bin/env python
# coding: utf-8

# In[1]:


########### Python Form Recognizer Async Analyze - WITH SYS ARGS #############
# importing necessary libraries 
import os #Used for Directory level operations in the code such as accessing directories
import os.path #Used for Directory level operations in the code such as accessing directories
from os import path #Used for Directory level operations in the code such as accessing directories
import sys #Used for Directory level operations in the code such as accessing directories
import json #Used for JSON parsing
import time #Leveraged for Logging
from requests import get, post #Leveraged for Initiating Requests
import ntpath #Leveraged for Parsing File Names
import re #Leevraged for Parsing File Names
from datetime import datetime

#PDF CONVERSION BASED LIBRARIES
from PIL import Image #Leveraged for reading input images when they're converted to DPFs
from fpdf import FPDF #Text File Creation and Merge Related Dependancies
from PyPDF2 import PdfFileMerger #Text File Creation and Merge Related Dependancies

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
        print("ERROR: " + "(INITIALIZATION)Log file creation has failed: \n%s" % str(e) + "\nCheck File Path in Config File")
    
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
                        msg = "Analysis succeeded:\n%s" + "\n" + "Exiting From Try Loop"
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
                
                #Reading JSON Setting for Merging################
                output_format = settings['output_format']
                
                #Trace-Activity
                msg = "Attempting to create Output with format: " + output_format
                logger(log_file, log_object, log_lev, "OUTPUT", msg)
                
                #Outputting to Required Format
                if output_format == "JSON":
                    #Calling Text Output Function to Generate RAW JSON Output File
                    txt_flag = generate_file(output_format, extension, txt_output, source,  output_path, file_name, log_file, log_object, log_lev)
                    
                elif output_format == "TXT" or output_format == "PDF_MERGE":
                    #WRITE CODE HERE
                    #Trace-Activity - STARTING JSON PARSING
                    msg = "Starting JSON Parsing"
                    logger(log_file, log_object, log_lev, "JSON-PARSE", msg)

                    #JSON PARSING FOR VALUES AS PER CONFIGURED FIELDS IN THE CONFIG FILE
                    #Importing Field Values from Settings File
                    field_array = settings['model_info']['fields_scope']
                    field_len = len(field_array)

                    #Trace-Activity
                    msg = str(field_len) + " fields found for scope of parsing from settings file"
                    logger(log_file, log_object, log_lev, "JSON-PARSE", msg)

                    #Importing output_key_prefix variable from JSON
                    output_key_prefix = settings['output_key_prefix']
                    
                    #################PARSING JSON#########################
                    txt_output = json_parse(data_result, field_array, field_len, log_file, log_object, log_lev, output_key_prefix)
                    
                    #Calling Text Output Function to Generate Output File Based on outputformat defined
                    output_create_flag, output_create_msg = generate_file(output_format, extension, txt_output, source,  output_path, file_name, log_file, log_object, log_lev)
                    
                    #Checking for appropriate Output Creation
                    if output_create_flag == True:
                        msg = "Create Output Type function has successfully executed"
                        logger(log_file, log_object, "TRACE", "MAIN - PDF-OUTPUT", msg)
                    else:
                        # Trace-Acitivity
                        msg = "Create Output Type function has successfully executed: " + "\n" + output_create_msg
                        logger(log_file, log_object, "TRACE", "MAIN - PDF-OUTPUT", msg)
                else:
                    #ERROR-Activity-UNKNOWN-FORMAT
                    msg = "Unknown Output Format defined in Settings File" + "\n" + "Quitting Script Now"
                    logger(log_file, log_object, "ERROR", "UNKNOWN-OUTPUT", msg)
                
                #Deleting Original File Based on FLag
                delete_flag = settings['delete_original_files']
                
                if delete_flag == "Yes":
                    os.remove(source)  
                    #Trace-Activity
                    msg = "Successfully Deleted File: " + source
                    logger(log_file, log_object, log_lev, "PDF-CONVERT", msg)
                    
                msg = "E2E Document Extraction and File Generation Function has Executed."
                logger(log_file, log_object, "TRACE", "FINAL-MESSAGE", msg)
                        
                
        #Closing Log File Session
        log_object.close()
 

#JSON PARSING FUNCTION
def json_parse(data_result, field_array, field_len, log_file, log_object, log_lev, output_key_prefix):
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
                line_item = output_key_prefix + field_in_scope + " : " + field_text
                txt_output = txt_output + line_item + "\n"

        except Exception as e:
            #Error-Activity
            parsemsg = "JSON Parsing Failed:\n%s" % str(e)
            logger(log_file, log_object, "ERROR", "JSON-PARSE", parsemsg)
    #ReturnOutput Text
    return txt_output


#Function for Outputting Text
def generate_file(output_format, extension, txt_output, source,  output_path, file_name, log_file, log_object, log_lev):
    try:
        
        if output_format == "JSON":
            #Creating Output JSON File
            json_file = output_path + "/" + file_name + ".json"
            file_object = open(txt_file,"w+")

            with open(json_file, "w") as outfile: 
                json.dump(data_result, outfile) 

            #Trace-Activity
            msg = "Successfully created JSON in: " + json_file
            logger(log_file, log_object, "TRACE", "FILE-OUTPUT-JSON", msg)
            
            #setting text flag as true
            output_create_flag = True
            output_create_msg = "Desired Output Format: " + output_format + ", has been successfully built in path: " + output_path
        
        elif output_format == "TXT":
            #Creating Output Text File
            txt_file = output_path + "/" + file_name + ".txt"
            file_object = open(txt_file,"w+")

            # the result is a Python dictionary:
            file_object.write(txt_output)
            file_object.close()

            #Trace-Activity
            msg = "Successfully created text file and logged data in: " + txt_file
            logger(log_file, log_object, "TRACE", "FILE-OUTPUT-TXT", msg)
            
            #setting text flag as true
            output_create_flag = True
            output_create_msg = "Desired Output Format: " + output_format + ", has been successfully built in path: " + output_path
        
        elif output_format == "PDF_MERGE":
            ###########CONVERTING ORIGINAL FILE TO PDF########################## 
            #Trace-Activity
            msg = "Iniitaiting Merge of Text Output with Original PDF and outputting file to: " + output_path
            logger(log_file, log_object, "TRACE", "FILE-OUTPUT-PDF_MERGE", msg)
            
            #Variables Pre-Defined or Need to Be Defined
            merged_pdf_path = output_path + "/" + file_name + "_merge" + ".pdf"
            txt_pdf_path = output_path + "/" + file_name + "_txt" + ".pdf"
            
            ###CONVERTING TO PDF FUNCTION IF FILE IS AN IMAGE
            #Checking whether extension is PDF or Image
            if extension != "pdf":
                #TRACE ACTIVITY HERE
                #Trace-Activity
                msg = "Original Image will be converted to pdf with output path" + str(output_path)
                logger(log_file, log_object, "TRACE", "FILE-OUTPUT-PDF_MERGE (PDF-CONVERT)", msg)

                convert_to_pdf_flag, convert_to_pdf_msg, output_pdf_path = convert_to_pdf(source, file_name)
                if convert_to_pdf_flag == True:
                    #Source will now be manipulated to point to the PDF file
                    source = output_pdf_path
                    #TRACE ACTIVITY HERE
                    msg = convert_to_pdf_msg + " to path: " + str(output_pdf_path)
                    logger(log_file, log_object, "TRACE", "FILE-OUTPUT-PDF_MERGE (PDF-CONVERT)", msg)
                else:
                    #TRACE ACTIVITY HERE 
                    logger(log_file, log_object, "ERROR", "FILE-OUTPUT-PDF_MERGE (PDF-CONVERT)", convert_to_pdf_msg + " Script will quit")
                    quit()

            #LOGGING MESSAGE HERE FOR INITIATING TEXT FILE OUTPUT AND PDF MERGING
            msg = "Initiating Conversion of Text File to PDF"
            logger(log_file, log_object, "TRACE", "FILE-OUTPUT-PDF_MERGE", msg)
            
            #INITIAZTING FUNCTION FOR TEXT TO PDF CONVERSION
            txt_pdf_out_flag, txt_pdf_out_msg = txt_pdf_out(txt_output, txt_pdf_path)

            if txt_pdf_out_flag == True:
                #TRACE ACTIVITY _  MESSAGE HERE FOR TRUE TEXT OUT
                logger(log_file, log_object, "TRACE", "FILE-OUTPUT-PDF_MERGE (TXT-OUTPUT)", txt_pdf_out_msg + "\n" + "Initiaziling PDF Merge Function")
                
                #INITIALIZING PDF MERGE FUNCTION
                pdf_merge_flag, pdf_merge_msg = pdf_merge(merged_pdf_path, source, txt_pdf_path)

                #Checking if Flag was successful
                if pdf_merge_flag == True:
                    #TRACE ACTIVITY -  MESSAGE HERE FOR TRUE PDF MERGE
                    logger(log_file, log_object, log_lev, "FILE-OUTPUT-PDF_MERGE (FINAL-MERGE)", pdf_merge_msg)
                else:
                    #TRACE ACTIVITY -  MESSAGE HERE FOR TRUE PDF MERGE
                    logger(log_file, log_object, "ERROR", "FILE-OUTPUT-PDF_MERGE (FINAL-MERGE)", pdf_merge_msg)
                
                #setting text flag as true
                output_create_flag = True
                output_create_msg = "Desired Output Format: " + output_format + ", has been successfully built in path: " + output_path
            else:
                #TRACE ACTIVITY _  MESSAGE HERE FOR TRUE TEXT OUT
                logger(log_file, log_object, "ERROR", "FILE-OUTPUT-PDF_MERGE (TXT-OUTPUT)", txt_pdf_out_msg)
                
                #setting text flag as true
                output_create_flag = False
                output_create_msg = "Output File Type: " + output_format + "Creation Failed: " + msg + "\n" + "Quitting Script Now"
    
    except Exception as e:
        msg = str(e)
        #Setting FLag as False
        output_create_flag = False
        #ERROR-Activity
        output_create_msg = "Output File Type: " + output_format + "Creation Failed: " + msg + "\n" + "Quitting Script Now"
        logger(log_file, log_object, "ERROR",output_create_msg)
    
    #Returning Text File Creation as Output
    return output_create_flag, output_create_msg   
    
def txt_pdf_out(txt_output, txt_pdf_path):
    try:  
        #Initiazing FPDF class
        txt_pdf = FPDF() 
        # Add a page 
        txt_pdf.add_page() 
        #Setting size and font
        txt_pdf.set_font('Arial', 'B', 14) 
        # create a cell 
        txt_pdf.multi_cell(w = 0, h = 10, txt = txt_output, border = 0, align = 'J', fill = False)

        # save the pdf with name .pdf 
        txt_pdf.output(txt_pdf_path)
        #Setting  Flag
        txt_pdf_out_flag = True 
        txt_pdf_out_msg = "Successfully Created PDF in path: " + txt_pdf_path
    except Exception as e:
        txt_pdf_out_msg = "Text Output PDF Creation Failed: " + "\n" + str(e)
        txt_pdf_out_flag = False
    
    #Returning Output Variables
    return txt_pdf_out_flag, txt_pdf_out_msg
    
def pdf_merge(merged_pdf_path, source_file, append_file):
    try:    
        #List of Files to be Merged
        merge_files = [source_file, append_file]
        merge_obj = PdfFileMerger()

        for files in merge_files:
            merge_obj.append(files)
        if not os.path.exists(merged_pdf_path):
            merge_obj.write(merged_pdf_path)
        merge_obj.close()

        #delete appended pdf
        os.remove(append_file)
        
        #Setting Flag and Message
        pdf_merge_flag = True
        pdf_merge_msg = "PDF Successfully Merged and Saved at: " + merged_pdf_path

    except Exception as e:
        pdf_merge_msg = "PDF Merge Failed: " + "\n" + str(e)
        pdf_merge_flag = False
    
    #Returning Output Variables
    return pdf_merge_flag, pdf_merge_msg

def convert_to_pdf(source, file_name):
    try:
        #Creating Output Path for Converted PDF
        output_pdf_path = os.path.dirname(source) + "/" + file_name + ".pdf"
        print(output_pdf_path)

        #opening image 
        image = Image.open(source)
        width, height = image.size
        
        #Initializing PDF
        img_pdf = FPDF(unit = "pt", format = [width, height])

        img_pdf.add_page()
        img_pdf.image(source,0,0)
        img_pdf.output(output_pdf_path, "F")

        #Setting Flag as True
        convert_to_pdf_flag = True
        convert_to_pdf_msg = "Image Successfully Converted"
    except Exception as e:
        convert_to_pdf_msg =  "Image to PDF Conversion Failed: " + "\n" + str(e)
        convert_to_pdf_flag = False
    
    #Returning Output Variables
    return convert_to_pdf_flag, convert_to_pdf_msg, output_pdf_path

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