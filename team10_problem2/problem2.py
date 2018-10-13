
import logging
import logging.handlers

#directory of log file;
logfile = './logger.log'
#config
logging.basicConfig(filename=logfile, level=logging.INFO)

#write in initial log file
logging.debug('debug message')
logging.info('info message')
logging.warning('warn message')
logging.error('error message')
logging.critical('critical message')

#create a handler with format to write into log file
handler = logging.StreamHandler(stream = None)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)

#logger object with a handler
logger = logging.getLogger('logging')
logger.addHandler(handler) 
logger.setLevel(logging.DEBUG)

logger.info('a info message')



import requests, zipfile, io
import os
import glob
import pandas as pd
import csv
import boto.s3
import time
import sys

timestr = time.strftime("%Y%m%d-%H%M%S")
print("Assignment1 Problem2, with Python.")


#Check the AWS Key First
argLen=len(sys.argv)

accessKey=''
secretAccessKey=''
year=''

for i in range(1,argLen):
    val=sys.argv[i]
    if val.startswith('accessKey='):
        pos=val.index("=")
        accessKey=val[pos+1:len(val)]
        continue
    elif val.startswith('secretKey='):
        pos=val.index("=")
        secretAccessKey=val[pos+1:len(val)]
        continue
    elif val.startswith('year='):
        pos=val.index("=")
        year=val[pos+1:len(val)]
        continue

print("Access Key=",accessKey)
print("Secret Access Key=",secretAccessKey)
print("Year = ",year)
#Check if keys are valid
if not accessKey or not secretAccessKey:
    logging.warning('Access Key or Secret Access Key not provided!!')
    print('Please provide both Access Key and Secret Access Key!!')
    exit()

AWS_ACCESS_KEY_ID = accessKey
AWS_SECRET_ACCESS_KEY = secretAccessKey

try:
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY)

    print("Connected to S3")
    bucket_name = 'team10ads'+timestr.replace(" ", "").replace("-", "").replace(":","").replace(".","")
    bucket = conn.create_bucket(bucket_name)
    print("Create the bucket %s",bucket_name)
except:
    logging.info("AWS keys are invalid!!")
    print("AWS keys are invalid!!")
    exit()

#if no data folder in the root directory, create one.
if not os.path.exists("data"):
    os.makedirs("data")

#check if year is valid 
valid_years = range(2003,2018)

if not year:
    year = 2003
    logging.warning('Program running for 2003 by default since you did not enter any Year.')

if int(year) not in valid_years:
    logging.error("Invalid year. Please enter a valid year between 2003 and 2017.")
    exit()

'''
def getYear():
    while True:
        try:
            year = int(input("Please enter a year: "))
            if year in range(2003,2018): 
                logger.info("Accepted.")
                return year
            else:
                logger.info("Please input a valid year.")
                continue
        except ValueError:
            logger.info("Please input a valid year.")
            continue
        else:
            break
'''

# return the string stand for quarter.
def getQuarter(month):
    if month in range(1,4): return 1
    if month in range(4,7): return 2
    if month in range(7,10): return 3
    if month in range(10,13): return 4
    

#get the url list need to download
def getUrlList(year):
    
    #local vairable as reference for return value.
    urlList = []
    
    #save the current year folder as "folder"
    
    folder = os.path.exists("data/%s/%s"%(year,timestr))
    
    #if the "folder" does not exist, create one.
    if not folder:
        os.makedirs("data/%s/%s"%(year,timestr))
        
        # the url consists 4 variable substrings, that is,
        # http://........%year......%quarter....%year%month....
        for month in range(1,13):
            quarter = getQuarter(month)
            if month < 10 : 
                month = '0{}'.format(month)
            urlList.append("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/%s/Qtr%s/log%s%s01.zip"%(year,quarter,year,month))
        
        #pull data from the urls
        for url in urlList:
            r = requests.get(url)
            try:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall("data/%s/%s"%(year,timestr))
                logger.info("extracting...")
            
            except:
                logger.info("Readme file downloaded.")
        
        logger.info("Done.")

    else :
        logger.info("Folder exists.")
        
    return urlList

getUrlList(year)

from collections import Counter

#clear the empty files from the filelist
#file list comes from glob
def filesOfValue():
    os.chdir("data/%s/%s"%(year,timestr))
    fileList = glob.glob('*01.csv')
    for file in fileList:
        if os.path.getsize(file) <= 93:# fsize = os.path.getsize(“data/20030101.csv”)
            fileList.remove(file)
    logger.info("fileList cleared.")
    for file in fileList:
        print(file)
    return fileList


def dataClean():
    fileList = filesOfValue()
    for file in fileList:
        df = pd.read_csv(file)

        #remove rows which have no ip, date, time, cik or accession
        df.dropna(subset=['cik'])
        df.dropna(subset=['accession'])
        df.dropna(subset=['ip'])
        df.dropna(subset=['date'])
        df.dropna(subset=['time'])

        #replace nan with 'unknown' in browser.
        df['browser'] = df['browser'].fillna('unknown')

         # replace nan idx with most frequent idx
        max_idx = pd.DataFrame(df.groupby('idx').size().rename('cnt')).idxmax()[0]
        df['idx'] = df['idx'].fillna(max_idx)

        # replace nan code with most frequent code
        max_code = pd.DataFrame(df.groupby('code').size().rename('cnt')).idxmax()[0]
        df['code'] = df['code'].fillna(max_code)
        
         # replace nan norefer with 0
        df['norefer'] = df['norefer'].fillna('1')
        
        # replace nan noagent with 0
        df['noagent'] = df['noagent'].fillna('1')
        
        # replace nan find with most frequent find
        max_find = pd.DataFrame(df.groupby('find').size().rename('cnt')).idxmax()[0]
        df['find'] = df['find'].fillna(max_find)
        
        # replace nan crawler with zero
        df['crawler'] = df['crawler'].fillna('0')
        
        # replace nan extention with most frequent extention
        max_extention = pd.DataFrame(df.groupby('extention').size().rename('cnt')).idxmax()[0]
        df['extention'] = df['extention'].fillna(max_extention)
        
        # replace nan extention with most frequent extention
        max_zone = pd.DataFrame(df.groupby('zone').size().rename('cnt')).idxmax()[0]
        df['zone'] = df['zone'].fillna(max_zone)
    
        # find mean of the size and replace null values with the mean
        df['size'] = df['size'].fillna(df['size'].mean(axis=0))

        #cleaning size 
        import numpy as np
        Percentile = np.percentile(df['size'],[0,25,50,75,100])
        iqr = Percentile[3] - Percentile[1]
        upLimit = Percentile[3]+iqr*1.5
        downLimit = Percentile[1]-iqr*1.5
        
        for i in df['noagent']:
            if (i<downLimit) | (i>upLimit):
                i = 0
                logger.info("anomaly handled")

        #writing to new csv file
        filename = "%simput.csv"%file[:-4]
        df.to_csv(filename)
        logger.info("Imputing...")
    logger.info("Done.")

dataClean()




# In[17]:



# to summary matrics file
def toSummary():
    filename = 'summary_matrics.csv'
    columnNames = ['Number of IP','Most frequent IP','Number of CIK','Most frequent CIK','Most frequent extention','Mean size','Most popular browser']
    dfmatrics = pd.DataFrame(columns=columnNames)
    
    for file in glob.glob("*imput.csv"):
        df = pd.read_csv(file)
        # define each column in summary matrics

        #number of unique ip address
        numOfIp = df['ip'].nunique()
        
        # most frequent ip address
        maxIp = pd.DataFrame(df.groupby('ip').size().rename('cnt')).idxmax()[0]

        # number of unique CIK 
        numOfCIK = df['cik'].nunique()

        # most frequent cik 
        maxCIK = pd.DataFrame(df.groupby('cik').size().rename('cnt')).idxmax()[0]

        # most frequent extention
        maxExtention =  pd.DataFrame(df.groupby('extention').size().rename('cnt')).idxmax()[0]

        # mean size 
        meanSize = df['size'].mean(axis=0)

        # most popular browser
        maxBrowser = pd.DataFrame(df.groupby('browser').size().rename('cnt')).idxmax()[0]

        data = [numOfIp,maxIp,numOfCIK,maxCIK,maxExtention,meanSize,maxBrowser]
        dfmatrics.loc['Month %s'%file[7:9]] = data
        
       # dff = pd.DataFrame(data,columns=['Number of IP','Most frequent IP','Number of CIK','Most frequent CIK','Most frequent extention','Mean size','Most popular browser'],index=['Month %s'%file[7:8]])

    print(dfmatrics)
    dfmatrics.to_csv(filename)
    
toSummary()


def joincsv():
    if not os.path.exists("%s%sall.csv"%(year,timestr)):
        
        #datalist for all records.
        dataList = []
        
        #csv file for all records.
        joinedCSV = "%s%sall.csv"%(year,timestr)
        #
        columnNames = ["ip","date","time","zone","cik","accession","extention","code","size","idx","norefer","noagent","find","crawler","browser"]
        for filename in glob.glob("*imput.csv"):
            data = pd.read_csv(filename,header = 0,delimiter="\t",low_memory = False)
            logger.info(filename + "...")
            
            dataList.append(data)
            
        logger.info("wait...")
        
        concatDf = pd.DataFrame(columns=columnNames)
        concatDf = pd.concat(dataList ,axis = 0)
        concatDf.to_csv(joinedCSV,index = None)
        logger.info("csv joined.")
        
    else:
        logger.info("joinded already.")
 
 
 
joincsv()

## Zip log file, joined csv file and summary matrics fill to a zip folder
def zipResult():
    join_csv_path = '%s%sall.csv'%(year,timestr)
    matrics_path = 'summary_matrics.csv'
    log_path = 'logger.log'
    zipf = zipfile.ZipFile('Team10Problem2.zip', 'w', zipfile.ZIP_DEFLATED)
    zipf.write(os.path.join(join_csv_path))
    zipf.write(os.path.join(matrics_path))

    if  os.path.exists("data"):
        os.chdir("data")
    
    os.chdir('..')
    os.chdir('..')
    os.chdir('..')
    zipf.write(os.path.join(log_path))

   
    logger.info("zip all successfully!")

zipResult()

## Upload zip file to Amazon S3 ##

from boto.s3.key import Key

os.chdir("data/%s/%s"%(year,timestr))

zipName = 'Team10Problem2.zip'
print ("Uploading %s to Amazon S3 bucket %s"%(zipName, bucket_name))

k = Key(bucket)
k.key = 'Team10Problem2'
k.set_contents_from_filename(zipName)

