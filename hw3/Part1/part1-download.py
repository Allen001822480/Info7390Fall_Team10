############### Import Libraries ###############
import requests
from lxml import html
import os
import sys
import logging # for logging
import shutil #to delete the directory contents
import zipfile

############### Initializing logging file ###############
root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch1 = logging.FileHandler('midterm_part1_log.log') #output the logs to a file
ch1.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch1.setFormatter(formatter)
root.addHandler(ch1)

ch = logging.StreamHandler(sys.stdout ) #print the logs in console as well
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

############### Cleanup required directories ###############
def cleanup_dir():
    try:
        if not os.path.exists('downloaded_zips'):
            os.makedirs('downloaded_zips', mode=0o777)
        else:
            shutil.rmtree(os.path.join(os.path.dirname(__file__),'downloaded_zips'), ignore_errors=False)
            os.makedirs('downloaded_zips', mode=0o777)
        
        if not os.path.exists('downloaded_zips_unzipped'):
            os.makedirs('downloaded_zips_unzipped', mode=0o777)
        else:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'downloaded_zips_unzipped'), ignore_errors=False)
            os.makedirs('downloaded_zips_unzipped', mode=0o777)
        
        if not os.path.exists('cleanFiles'):
            os.makedirs('cleanFiles', mode=0o777)
        else:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'cleanFiles'), ignore_errors=False)
            os.makedirs('cleanFiles', mode=0o777) 
        if not os.path.exists('cleanFilesWithSummaries'):
            os.makedirs('cleanFilesWithSummaries', mode=0o777)  
        else:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'cleanFilesWithSummaries'), ignore_errors=False)
            os.makedirs('cleanFilesWithSummaries', mode=0o777) 
        logging.info('Directories cleanup complete.')
    except Exception as e:
        print(str(e))
        exit()
    

############### Create Session ###############
def data_download(uname,pwd):
    USERNAME='gao.shu@husky.neu.edu'
    PASSWORD='gj{vx<Io'
    print('username='+uname)
    print('password='+pwd)
    
    payload = {
        "username": USERNAME, 
        "password": PASSWORD
    }
    
    
    session_requests = requests.session()
    
    login_url = "https://freddiemac.embs.com/FLoan/secure/auth.php"
    
    result = session_requests.post(
        login_url, 
        data = payload, 
        headers = dict(referer=login_url)
    )
    
    url = 'https://freddiemac.embs.com/FLoan/Data/download.php'
    agreement_payload={
        "accept":"Yes",
        "action":"acceptTandC",
        "acceptSubmit":"Continue"
        }
    result = session_requests.post(
        url, 
        agreement_payload,
        headers = dict(referer = url)
    )
    
    tree = html.fromstring(result.content)
    all_links = tree.findall(".//a")
    
    ############### Download zips ###############
    for link in all_links:
        href=link.get("href")
        if "sample" in href:
          if int(link.text[-8:-4]) >= 2005:
            url= 'https://freddiemac.embs.com/FLoan/Data/'+href
            r = session_requests.get(url,stream=True)
            with open(os.path.join('downloaded_zips',link.text), 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
    
    
    ############### Unzip and extract text files ###############
    try:
        zip_files = os.listdir('downloaded_zips')
        for f in zip_files:
            z = zipfile.ZipFile(os.path.join('downloaded_zips', f), 'r')
            for file in z.namelist():
                if file.endswith('.txt'):
                    z.extract(file, r'downloaded_zips_unzipped')
        logging.info('Zip files successfully extracted to folder: downloaded_zips_unzipped.')
    except Exception as e:
            logging.error(str(e))
            exit()

############### Import Libraries ###############
import os
import glob
import pandas as pd
import numpy as np
import sys

cleanup_dir()
arg_len=len(sys.argv)
uname=''
pwd=''
if arg_len == 1:
    print("Arguments not entered will run for default")
    uname='gao.shu@husky.neu.edu'
    pwd='gj{vx<Io'
elif arg_len == 3:
    uname=sys.argv[1]
    pwd=sys.argv[2]
else:
    uname='gao.shu@husky.neu.edu'
    pwd='gj{vx<Io'

data_download(uname,pwd)


col_names_orig=['credit_score', 'first_pay_date', 'first_time_homebuyer',
                'maturity_date', 'msa', 'mi_percentage', 'no_of_units','occupance_status',
                'original_cltv', 'original_dti_ratio', 'original_upb', 'original_ltv',
                'original_interest_rate', 'channel', 'ppm_flag', 'product_type',
                'property_state', 'property_type', 'postal_code', 'loan_sequence_no', 
                'loan_purpose', 'original_loan_term', 'no_of_borrowers', 'seller_name',
                'servicer_name', 'super_conforming_flag','pre_harp_loan_sequence_no']

col_names_svcg=['loan_sequence_no', 'monthly_reporting_period', 'current_actual_upb', 'current_loan_delinquency_status',
                  'loan_age', 'remaning_months_on_legal_maturity', 'repurchase_flag', 'modification_flag', 'zero_bal_code',
                  'zero_bal_eff_date', 'current_interest_rate', 'current_deferred_upb', 'ddlpi', 'mi_recoveries', 'net_sales_proceeds',
                  'non_mi_recoveries', 'expenses', 'legal_costs', 'maintenance_preservation_cost', 'taxes_insurance', 'misc_expenses',
                  'actual_loss_calc', 'modification_cost','step_modification_flag','deferred_payment_modification,','estimated_loan_to_value']




####### Fnc to read each orig file, clean it and save the cleaned file ##########
def clean_orig_files():
    try:
        orig_filelists = glob.glob('downloaded_zips_unzipped' + "/sample_orig*.txt")
        for file in orig_filelists:
            
            data =pd.read_csv(file, sep='|', names= col_names_orig, low_memory=False)

            data = data.drop(columns = ['super_conforming_flag','pre_harp_loan_sequence_no'])
            # remove rows contain more than 4 NaN
            data.dropna(thresh=4)
            #remove rows doesn't have loan_sequence_no
            data = data[pd.notnull(data['loan_sequence_no'])]
            # replace credit_score with mode
            data.credit_score=data.credit_score.replace(r'\s+', np.nan, regex=True).astype('float64')
            cs = pd.DataFrame(data['credit_score'])
            mode=cs.mode()
            data['credit_score'] = data['credit_score'].fillna(mode.iloc[0]['credit_score'])
        
            # replace unknown value of msa with 0000
            data.msa=data.msa.replace(r'\s+', np.nan, regex=True)
            data['msa'] = data['msa'].fillna('00000').astype('float64')
        
            # replace unknown value of mi_percentage with 0
            data.mi_percentage=data.mi_percentage.replace(r'\s+', np.nan, regex=True).astype('float64')
            data['mi_percentage'] = data['mi_percentage'].fillna(0)
        
            # replace unknown value of no_of_units with 1
            data.no_of_units=data.no_of_units.replace(r'\s+', np.nan, regex=True).astype('float64')
            data['no_of_units'] = data['no_of_units'].fillna(1)
        
            #replacing unknown value of occupance_status with "O" (mode)
            data.occupance_status=data.occupance_status.str.strip()
            data['occupance_status'] = data['occupance_status'].fillna('O')
        
            #replacing unknown value of original_cltv with mean
            data.original_cltv=data.original_cltv.replace(r'\s+', np.nan, regex=True).astype('float64')
            df = pd.DataFrame(data['original_cltv'])
            mean = df.mean()
            data['original_cltv'] = data['original_cltv'].fillna(mean['original_cltv'])
            
            #replacing unknown value of original_dti_ratio with mean
            data.original_dti_ratio = data.original_dti_ratio.replace(r'\s+', 66, regex=True).astype('float64')
            data['original_dti_ratio'] = data['original_dti_ratio'].fillna(data['original_dti_ratio'].mean())
            
            #replacing unknown value of original_ltv with mean
            data.original_ltv=data.original_ltv.replace(r'\s+', np.nan, regex=True).astype('float64')
            df = pd.DataFrame(data['original_ltv'])
            mean=data.mean()
            df['original_ltv'] = df['original_ltv'].fillna(mean)
        
            #replacing unknown channel with mode
            data['channel'] = data['channel'].fillna(data['channel'].mode())
        
            #replacing with N as unknown value
            data.ppm_flag=data.ppm_flag.str.strip()
            data['ppm_flag'] = data['ppm_flag'].fillna('N')
        
            #replaceing with mode as unknown value
            data['property_type'] = data['property_type'].fillna(data['property_type'].mode())
        
            #replacing with 00000 as unknown value
            data.postal_code=data.postal_code.replace(r'\s+', np.nan, regex=True)
            data['postal_code'] = data['postal_code'].fillna('00000')
        
            #replaceing with mode as unknown value
            data['loan_purpose'] = data['loan_purpose'].fillna(data['loan_purpose'].mode())
        
            #replaceing with mode as unknown value
            data['no_of_borrowers'] = data['no_of_borrowers'].fillna(data['no_of_borrowers'].mode())
        
            name = file[-20:-3] + 'csv' 
            data.to_csv(os.path.join('cleanFiles',name),index=False, sep=',', encoding='utf-8')
            print('Origination files cleaned.')
    except Exception as e:
        print(str(e))
        exit()

clean_orig_files()

####### Fnc to read each performance file, clean it and save the cleaned file ##########

def clean_svcg_files():
    try:
        svcg_filelists = glob.glob('downloaded_zips_unzipped' + "/sample_svcg*.txt")
        for file in svcg_filelists:
            data=pd.read_csv(file, sep='|', names= col_names_svcg, low_memory=False)
            #remove rows contain NaN loan_sequence_no
            data = data[pd.notnull(data['loan_sequence_no'])]

            #remove rows contain NaN monthly_report_period
            data = data[pd.notnull(data['monthly_reporting_period'])]

            #remove rows contain NaN current_actual_upb
            data = data[pd.notnull(data['current_actual_upb'])]

            #remove rows contain NaN current_loan_delinquency_status
            data = data[pd.notnull(data['current_loan_delinquency_status'])]

            #remove rows contain NaN loan_age
            data = data[pd.notnull(data['loan_age'])]

            #remove rows contain NaN remaning_months_on_legal_maturity
            data = data[pd.notnull(data['remaning_months_on_legal_maturity'])]

            #remove cols which has too many NaN
            data = data.drop(columns = ['repurchase_flag','modification_flag','zero_bal_code',
            'zero_bal_eff_date','ddlpi','mi_recoveries', 'net_sales_proceeds','non_mi_recoveries','expenses','legal_costs','maintenance_preservation_cost','taxes_insurance','misc_expenses',
            'actual_loss_calc','modification_cost','step_modification_flag','deferred_payment_modification,'])
            
            #replace unknown with 0
            data.estimated_loan_to_value=data.estimated_loan_to_value.replace(r'\s+', np.nan, regex=True)
            data['estimated_loan_to_value'] = data['estimated_loan_to_value'].fillna('0').astype('float64')

            name=file[-20:-3] + 'csv'
            data.to_csv(os.path.join('cleanFiles',name), index=False)
            print('Performance files cleaned.')
    except Exception as e:
        print(str(e))
        exit()

clean_svcg_files()

###################### Combine and Summaries of Origination file #################
from collections import OrderedDict 
def combine_summary_orig():
    try:
        orig_summary = []
        dataList = []
        fname = 'summary_orig.csv'
        file_name = 'orig_join.csv'
        colNames = ['credit_score', 'first_pay_date', 'first_time_homebuyer',
                'maturity_date', 'msa', 'mi_percentage', 'no_of_units','occupance_status',
                'original_cltv', 'original_dti_ratio', 'original_upb', 'original_ltv',
                'original_interest_rate', 'channel', 'ppm_flag', 'product_type',
                'property_state', 'property_type', 'postal_code', 'loan_sequence_no', 
                'loan_purpose', 'original_loan_term', 'no_of_borrowers', 'seller_name',
                'servicer_name','year','quarter']
        clean_filelists = glob.glob('cleanFiles' + "/sample_orig*.csv")
        for file in clean_filelists:
            data = pd.read_csv(file)
            data['year'] = file[-8:-4]
            data['quarter'] = data.loan_sequence_no.str[4:6]
            
            dataList.append(data)

            summ_df = OrderedDict()
            summ_df['year'] = file[-8:-4]
            summ_df["aveCreditScore"] = data["credit_score"].mean()
            summ_df["loanCount"] = np.count_nonzero(data["loan_sequence_no"])
            summ_df["aveMI"] = data["mi_percentage"].mean()
            summ_df["aveUnitNumber"] = data["no_of_units"].mean()
            summ_df["aveCLTV"] = data["original_cltv"].mean() 
            summ_df["aveDTI"] = data["original_dti_ratio"].mean()
            summ_df["aveLTV"] = data["original_ltv"].mean()
            summ_df["totalUPB"] = data["original_upb"].sum() 
            summ_df["aveUPB"] = data["original_upb"].mean()
            summ_df["aveInterestRate"] = data["original_interest_rate"].mean()
            summ_df["mostFrequentState"] = pd.DataFrame(data.groupby('property_state').size().rename('cnt')).idxmax()[0]
            summ_df["mostFrequentType"] =  pd.DataFrame(data.groupby('property_type').size().rename('cnt')).idxmax()[0]
            summ_df["mostFrequentPostalCode"] =  pd.DataFrame(data.groupby('postal_code').size().rename('cnt')).idxmax()[0]
            summ_df["aveNumberOfBorrowers"] = data["no_of_borrowers"].mean()
            summ_df["mostFrequentSeller"] =  pd.DataFrame(data.groupby('seller_name').size().rename('cnt')).idxmax()[0]
            summ_df["mostFrequentServicer"] =  pd.DataFrame(data.groupby('servicer_name').size().rename('cnt')).idxmax()[0]

            orig_summary.append(summ_df)

        concatDf = pd.DataFrame(columns=colNames)
        concatDf = pd.concat(dataList ,axis = 0)
        concatDf.to_csv(os.path.join('cleanFilesWithSummaries',file_name),index=False)
        orig_summary = pd.DataFrame(orig_summary)
        orig_summary.to_csv(os.path.join('cleanFilesWithSummaries',fname),index=False)
        print('Originating files summaries and join created')
    except Exception as e:
        print(str(e))
        exit()    
       
    
combine_summary_orig()

     
###################### Combine and Summaries of Performance file #################
def combine_summary_svcg():
    try:
        svcg_summary = []
        dataList = []
        fname = 'summary_svcg.csv'
        file_name = 'svcg_join.csv'
        colNames = ['loan_sequence_no', 'monthly_reporting_period', 'current_actual_upb', 
        'current_loan_delinquency_status','loan_age', 'remaning_months_on_legal_maturity',
        'current_interest_rate', 'current_deferred_upb','estimated_loan_to_value','year','quarter']
        clean_filelists = glob.glob('cleanFiles' + "/sample_svcg*.csv")
        for file in clean_filelists:
            data = pd.read_csv(file)
            data['year'] = file[-8:-4]
            data['quarter'] = data.loan_sequence_no.str[4:6]

            dataList.append(data)

            summ_df = OrderedDict()
            summ_df['year'] = file[-8:-4]
            total_loans = data['loan_sequence_no'].nunique()
            summ_df['no_distinct_loans_per_year'] = data['loan_sequence_no'].nunique()
            summ_df['most_lenth_loan_months'] = data.groupby('loan_sequence_no').size().max()
            total_reos = data[data['current_loan_delinquency_status'].astype(str) == 'R']['loan_sequence_no'].nunique()
            summ_df['total_reos_per_year_percent'] = 100 * total_reos/total_loans
            summ_df['mean_upb_year'] = data['current_actual_upb'].mean()
            summ_df['avg_loan_age'] = data['loan_age'].mean()
            summ_df['avg_current_interest_rate'] = data['current_interest_rate'].mean()

            svcg_summary.append(summ_df)

        concatDf = pd.DataFrame(columns=colNames)
        concatDf = pd.concat(dataList ,axis = 0)
        concatDf.to_csv(os.path.join('cleanFilesWithSummaries',file_name),index=False)
        svcg_summary = pd.DataFrame(svcg_summary)
        svcg_summary.to_csv(os.path.join('cleanFilesWithSummaries',fname),index=False)
        print('Performance files summaries and join created')
    except Exception as e:
        print(str(e))
        exit()       

combine_summary_svcg()

