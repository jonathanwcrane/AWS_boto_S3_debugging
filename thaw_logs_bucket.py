#!/usr/bin/env python
#inventory a bucket

import boto.s3
from boto.s3.connection import OrdinaryCallingFormat
import re
import os
import time


dest_dict = {}
dest_count_dict = {}
exp_date_dict = {}
num_obj = 0
tot_scanned = 0
glacier_obj = 0
nonglacier = 0
nonmatch_count = 0
nonmatch_date_count = 0
matched_sc = 0
nonmatched_sc = 0
restore_count = 0
ongoing_restore = 0

def move_key(key):
    global nonmatch_count
    #global dest_count_dict
    contents = key.get_contents_as_string()
    found = False
    for ddkey in dest_count_dict:
        if re.search(str(ddkey),str(contents)):
            found = True
            #this way I can put this logic back in easily
            #if dest_count_dict[ddkey] % 1000 == 0:
            if True:
                #print("Moving",nm,"to",dest_dict[ddkey]+'/'+nm)
                #the preserve_acl causes an Access Denied error..not sure why
                #key.copy(bn,dest_dict[ddkey]+'/'+nm,preserve_acl=True,validate_dst_bucket=True)
                new_nm = re.sub('fgjk','',nm,)
                dest_nm = dest_dict[ddkey]+'/'+new_nm
                bucket.copy_key(dest_nm,bn,nm,storage_class='STANDARD')
                #key.copy(bn,dest_dict[ddkey]+'/'+nm,validate_dst_bucket=True)
                key.delete()
                dest_count_dict[ddkey] += 1
            #just save us from looping a second time if we hit it the first time ()
            break
    if (not found):
        print("Error:",nm,"does not match either search term.")
        nonmatch_count += 1
        if nonmatch_count % 100 == 0:
            print("-----------------contents of",nm)
            print(contents)
            
def status_report():
    print("****************STATUS REPORT*******************")
    print("Scanned",tot_scanned,"objects;")
    print(num_obj,"objects in root of bucket")
    print(nonglacier,"non-glacier objects")
    print(nonmatch_count,"non-matched objects")
    print(nonmatch_date_count,"objects with differing expiry dates")
    print("Began restore of",restore_count,"objects.")
    print(ongoing_restore,"objects are in the middle of being restored")
    #print("Began restore of",glacier_obj,"glacier objects;")
    for key in exp_date_dict:
        print(exp_date_dict[key],"are restored and expire on",key)
    for key in dest_count_dict:
        print("Moved",dest_count_dict[key],"objects from",key)
                       
#Not sure this applies here but keeping just in case
region = "us-east-1"
#The profile to use in the credentials or boto.config file, if used
profile_nm = 'pn'
#The env var to look in for the AWS KEY, if used
aws_key_env_var = 'AWS_KEY'
#The env var to look in for the secret key, if used
aws_secret_key_env_var = 'AWS_SECRET_KEY'
#First check for ENV VARs with AWS Credentials
#and make the connection
aws_key = os.environ.get(aws_key_env_var)
aws_secret_key = os.environ.get(aws_secret_key_env_var)
if (aws_key and aws_secret_key):
    print("Signing in using ENV VAR credentials")
    s3_conn = boto.connect_s3(aws_access_key_id=aws_key,aws_secret_access_key=aws_secret_key,calling_format=OrdinaryCallingFormat())
    #s3_conn = boto.connect_s3(aws_access_key_id=aws_key,aws_secret_access_key=aws_secret_key)
#If env vars don't exist, use the profile in the boto.config file
#If the env vars and profile both exist, the program will never look for the profile, and use the env vars.
else:
    print("Signing in using boto config credentials")
    s3_conn = boto.connect_s3(profile_name=profile_nm,calling_format=OrdinaryCallingFormat())
    #s3_conn = boto.connect_s3(profile_name=profile_nm)

bn = 'bn'
bucket = s3_conn.get_bucket(bn)
#2012-03-17
#trying this grammar/stuff to get only root stuff
for key in bucket.list(prefix='',delimiter='/'):
#for key in bucket.list(prefix='2014-03-'):
#for key in bucket.list(prefix='gfgsd'):
    #need to set to the string representation, otherwise we're comparing objects
    nm = key.name
    tot_scanned += 1
    num_obj += 1
    try:
        sc = key.storage_class
    except AttributeError:
        print(nm,"doesn't have a storage class because it's a prefix?????????????????")
        #break out because the storage class won't work
        break
    #This next line makes the script take 4.5 hours instead of 20 min
    key2 = bucket.get_key(nm)
    sc2 = key2.storage_class
    if sc != sc2:
        #print("Storage classes don't match for",nm)
        nonmatched_sc += 1
        if nonmatched_sc % 100 == 0:
            print("Storage classes don't match for",nm)
    else:            
        matched_sc += 1
    if sc2 == 'GLACIER':
        restore_status = key2.ongoing_restore
        #print(restore_status)
        if restore_status:
            ongoing_restore += 1
            #print(nm,"is still being restored.")
        else:
            #print("Restoring",nm)
            #This is throwing an error for some reason
            #key.restore(days=8)
            #hmm will this work?
            date1=key.expiry_date
            date2=key2.expiry_date
            if (date1 != date2):
                #if nonmatch_date_count % 1000 == 0:
                #    print("Expiry dates don't match for",nm)
                #    print("Date 1 is", date1,"date 2 is",date2)
                nonmatch_date_count += 1
            if date2:
                if date2 in exp_date_dict:
                    exp_date_dict[date2] += 1
                else:
                    exp_date_dict[date2] = 1
                move_key(key2)
            else:
                print("-----No expiry date for",nm,"so it's not thawed???????")
                #let's get a progress report for every Nth restore
                if restore_count % 1000 == 0:
                    print("We are on restore #",restore_count)
                    status_report()
                #try to catch the error here?
                try:
                    key2.restore(30)
                    restore_count += 1
                    #pass
                #This catches 'socket.gaierror: [Errno -3] Temporary failure in name resolution'
                #it probably catches other errors, too, but either way we'll sleep a couple winks
                except IOError:
                    print("------------------Got an error at restore #",restore_count,"Sleeping...")
                    time.sleep(2) 
                #pass
            glacier_obj += 1
            #pass
    else:
        #I believe this code will never be executed.
        nonglacier += 1
        move_key(key2)
            
status_report()