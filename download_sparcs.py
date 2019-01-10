#!/usr/bin/env python -W ignore

from absl import flags
from absl import app

import pandas as pd
import numpy as np
import sys
from sodapy import Socrata
from os.path import abspath
from os.path import exists
from os import mkdir

FLAGS = flags.FLAGS
# delcare flags
flags.DEFINE_string("token", None, "SPARCS Socrates API token")
flags.DEFINE_string("output", None, "Output directory to save files")

# required flags
flags.mark_flag_as_required("token")
flags.mark_flag_as_required("output")

apr_drg_codes = map(str, [])
ccs_diag_codes = map(str, [])
ccs_proc_codes = map(str, [])

columns_to_keep = map(lambda x: x.lower().replace(' ', '_'), [
    "APR Risk of Mortality",
    "APR Severity of Illness Code",
    "Age Group",
    "CCS Diagnosis Code",
    "Discharge Year",
    "Ethnicity",
    "Gender",
    "Length of Stay",
    "Patient Disposition",
    "Source of Payment 1",
    "Race",
    "Total Costs",
    "Total Costs_inflation_adjusted",
    "Type of Admission",
    'apr_drg_code'
])

pd_list = []

# for Hospital Inpatient Discharges (SPARCS De-Identified) in SPARCS
dataset_ids = [
    (2016, 'y93g-4rqn'),
    (2015, 'p3tf-wrj8'),
    (2014, 'pzzw-8zdv'),
    (2013, 'tdf6-7fpk'),
    (2012, 'rv8x-4fm3'),
    (2011, 'n5y9-zanf'),
    (2010, 'dpew-wqcg'),
    (2009, 's8d9-z734')
]

'''
Download all the datasets in dataset_ids and return a list of pd dataframes
'''
def download(token, verbose=True):
    if not isinstance(token, basestring):
        raise ValueError("Token must be a string")
        
    # Setup SPARCS API
    client = Socrata("health.data.ny.gov",
                    token)
    # set an arbitrarily high download limit
    # only works for python 2
    if sys.version_info < (3,0):
        lim = sys.maxint
    else:
        # hardcode max int for python 3
        lim = 9223372036854775807
    for id in dataset_ids:
        year = id[0]
        socrata_id = id[1]
        filter_command = ''
        # has apr_drg_description_and_code
        if year == 2011:
            filter_command = make_filter_command_by_year(year = 2011, 
                                                        ccs_diag = ccs_diag_codes, 
                                                        ccs_proc = ccs_proc_codes, 
                                                        apr_drg = apr_drg_codes)
        # apr_drg_code are integers
        elif year == 2015 or year == 2016:
            # years 2015 and 2016 are the same, so it doesn't matter which is passed into make_filter_command_by_year
            filter_command = make_filter_command_by_year(year = 2015, 
                                            ccs_diag = ccs_diag_codes, 
                                            ccs_proc = ccs_proc_codes, 
                                            apr_drg = apr_drg_codes)
        else:
            # year only matters if 2011, 2015, or 2016. Don't pass to force default behavior
            filter_command = make_filter_command_by_year(ccs_diag = ccs_diag_codes, 
                                            ccs_proc = ccs_proc_codes, 
                                            apr_drg = apr_drg_codes)
        print "Filter: %s" % str(filter_command)
        if verbose:
            sys.stdout.write('Downloading id: %s (%d) using filter...' % (socrata_id, year))
            sys.stdout.flush()
        http_get = client.get(socrata_id, limit=lim, where=filter_command)
        results_df = pd.DataFrame.from_records(http_get)
        if verbose:
            print 'Shape = {}'.format(results_df.shape)
        pd_list.append(results_df)
    return pd_list

def make_filter_command_by_year(year = 0, ccs_diag = None, ccs_proc = None, apr_drg = None):
    # SPARCS API format call changes by year
    command_filter = []
    if year == 2011:
        # correct format
        # """ccs_diagnosis_code='{ccs_diagnosis_code}' AND \
        #     ccs_procedure_code='{ccs_procedure_code}' AND \
        #     apr_drg_description_and_code='{apr_drg_code}'"""
        if ccs_diag != None and len(ccs_diag) >= 1:
            ccs_diag_codes = '('+' OR '.join(["ccs_diagnosis_code='%s'"%x for x in ccs_diag])+')'
            command_filter.append(ccs_diag_codes)
        if ccs_proc != None and len(ccs_proc) >= 1:
            ccs_proc_codes = '('+' OR '.join(["ccs_procedure_code='%s'"%x for x in ccs_proc])+')'
            command_filter.append(ccs_proc_codes)
        if apr_drg != None and len(apr_drg) >= 1:
            apr_drg_codes = '('+' OR '.join(["apr_drg_description_and_code='%s'"%x for x in apr_drg])+')'
            command_filter.append(apr_drg_codes)
        return ' AND '.join(command_filter)
    # ccs_diagnosis_code, ccs_procedure_code, apr_drg_code are integers (not quoted)
    elif year == 2015 or year == 2016:
        # Correct format
        # """ccs_diagnosis_code={ccs_diagnosis_code} AND \
        #     ccs_procedure_code={ccs_procedure_code} AND \
        #     apr_drg_code={apr_drg_code}"""
        if ccs_diag != None and len(ccs_diag) >= 1:
            ccs_diag_codes = '('+' OR '.join(["ccs_diagnosis_code=%s"%x for x in ccs_diag])+')'
            command_filter.append(ccs_diag_codes)
        if ccs_proc != None and len(ccs_proc) >= 1:
            ccs_proc_codes = '('+' OR '.join(["ccs_procedure_code=%s"%x for x in ccs_proc])+')'
            command_filter.append(ccs_proc_codes)
        if apr_drg != None and len(apr_drg) >= 1:
            apr_drg_codes = '('+' OR '.join(["apr_drg_code=%s"%x for x in apr_drg])+')'
            command_filter.append(apr_drg_codes)
        return ' AND '.join(command_filter)
    else:
        # Correct format
        # """ccs_diagnosis_code='{ccs_diagnosis_code}' AND \
        #    ccs_procedure_code='{ccs_procedure_code}' AND \
        #    apr_drg_code='{apr_drg_code}'"""
        if ccs_diag != None and len(ccs_diag) >= 1:
            ccs_diag_codes = '('+' OR '.join(["ccs_diagnosis_code='%s'"%x for x in ccs_diag])+')'
            command_filter.append(ccs_diag_codes)
        if ccs_proc != None and len(ccs_proc) >= 1:
            ccs_proc_codes = '('+' OR '.join(["ccs_procedure_code='%s'"%x for x in ccs_proc])+')'
            command_filter.append(ccs_proc_codes)
        if apr_drg != None and len(apr_drg) >= 1:
            apr_drg_codes = '('+' OR '.join(["apr_drg_code='%s'"%x for x in apr_drg])+')'
            command_filter.append(apr_drg_codes)
        return ' AND '.join(command_filter)

'''
Standardize column names across all datasets
'''
def standardizeColumns(list_of_dfs):
    df_list = []
    for df in list_of_dfs:
        colHeader = df.columns.values
        for index,val in enumerate(colHeader):
            ################
            # Rename medicare
            #2011 has a mislabeled column header, replace with correct
            if val == "payment_topology_2":
                df.columns.values[index] = "payment_typology_2"
            # replace typology with source of payment
            if val == "payment_typology_1":
                df.columns.values[index] = "source_of_payment_1"
            elif val == "payment_typology_2":
                df.columns.values[index] = "source_of_payment_2"
            elif val == "payment_typology_3":
                df.columns.values[index] = "source_of_payment_3"
            ##################
            # Rename apr_severity_of_illness_descript
            if val == 'apr_severity_of_illness_descript':
                df.columns.values[index] = 'apr_severity_of_illness_description'
            if val == 'apr_drg_description_and_code':
                df.columns.values[index] = 'apr_drg_code'
            if val == 'age':
                df.columns.values[index] = 'age_group'
            if val == 'apr_severity_of_illness':
                df.columns.values[index] = 'apr_severity_of_illness_code'
            if val == 'sex':
                df.columns.values[index] = 'gender'
            if val == 'operating_provider_license_numbe':
                df.columns.values[index] = 'operating_provider_license_number'
            if val == 'attending_provider_license_numbe':
                df.columns.values[index] = 'attending_provider_license_number'
        df_list.append(df)
    return df_list

'''
Corrects the headers and filter out patients who do not use medicare

NB: This MUST be called before the header spaces are replaced by _
^ Is not an issue if downloading from socrata since cols already have _
'''
def codeMedicare(df):
    medicare_bool = []
    for ndx, row in df.iterrows():
        _1 = row['source_of_payment_1'].lower() == 'medicare'
        try:
            _2 = row['source_of_payment_2'].lower() == 'medicare'
        except:
            _2 = False
        try:
            _3 = row['source_of_payment_3'].lower() == 'medicare'
        except:
            _3 = False
        bool = _1 | _2 | _3
        medicare_bool.append(bool)
    df['medicare'] = medicare_bool
    return df

def subsetMedicare(df):
    return df[df['medicare'] == True]

def assignNumeric(df):
    _TC = 'total_costs'
    _TCh = 'total_charges'
    _LOS = 'length_of_stay'
    _YEAR = 'discharge_year'
    # remove non-integer rows from LOS
    if df.dtypes[_LOS] == 'object':
        df = df[df[_LOS] != "120 +"]
    df[[_TC, _TCh, _LOS, _YEAR]] = df[[_TC, _TCh, _LOS, _YEAR]].apply(pd.to_numeric)
    return df

"""
Combines all dataframes into one master
"""
def combine_dataframes(pd_list):
    master = pd.concat(pd_list, ignore_index = True, axis=0, sort=False)
    master = master.fillna(0)
    return master

def adjustForInflation(df, column_input):
    # multiply cost in year by the multiplicative CPI rate according to the BLS
    # From: https://data.bls.gov/cgi-bin/cpicalc.pl?cost1=1.00&year1=201601&year2=200901
    inflationDictionary = {
        "2016":0.89,
        "2015":0.90,
        "2014":0.90,
        "2013":0.92,
        "2012":0.93,
        "2011":0.96,
        "2010":0.97,
        "2009":1.00
    }
    inflationList = [float(row[column_input])*inflationDictionary[str(row['discharge_year'])] for index,row in df.iterrows()]
    df[column_input + '_inflation_adjusted'] = inflationList
    return df

'''
Remove outliers from dataset by keeping drop_lower %ile to drop_upper%ile
'''
def removeOutliers(df, drop_lower=0.5, drop_upper=99.5):
    _TC = 'total_costs_inflation_adjusted'
    # convert all outcome rows to numerical if possible
    df[[_TC]] = df[[_TC]].apply(pd.to_numeric)

    #remove outliers
    # drop rows below 0.5th percentile and above 99.5th percentile
    TC_ulimit = np.percentile([float(x) for x in df[_TC]], drop_upper)
    TC_llimit = np.percentile([float(x) for x in df[_TC]], drop_lower)
    # LOS_ulimit = np.percentile([int(x) for x in df[_LOS]], drop_upper)
    # LOS_llimit = np.percentile([int(x) for x in df[_LOS]], drop_lower)
    print 'Upper limit: %s, lower limit: %s' % (TC_ulimit, TC_llimit)
    df = df.query('{} < {}'.format(_TC, TC_ulimit))
    df = df.query('{} > {}'.format(_TC, TC_llimit))
    # df = df.query('{} < {}'.format(_LOS, LOS_ulimit))
    # df = df.query('{} > {}'.format(_LOS, LOS_llimit))
    return df

def additional_cleaning(df):
    # do NOT clean age group for this paper
    # if "age_group" in df.columns:
    #     df1 = df[df.age_group == "70 or Older"]
    #     df2 = df[df.age_group == "50 to 69"]
    #     df = pd.concat([df1,df2],ignore_index=True, axis=0, sort=False)
    # do NOT clean admission for this paper
    # if "type_of_admission" in df.columns:
    #     df = df[df.type_of_admission != 'Newborn']
    #     df = df[df.type_of_admission != 'Not Available']
    # DO clean out dispositions
    if "patient_disposition" in df.columns:
        df = df[df.patient_disposition != "Left Against Medical Advice"]
        df = df[df.patient_disposition != "Expired"]
        df = df[df.patient_disposition != "Another Type Not Listed"]
    return df

def load_all_patients(output_dir):
    df = pd.read_csv('%s/%s' % (output_dir, 'all_patients.csv'))
    return df


def main(argv):
    output_dir = abspath(FLAGS.output)
    if not exists(output_dir):
        sys.out.write('[INFO] Making directory: %s' % output_dir)
        mkdir(output_dir)
    pd_list = download(FLAGS.token)
    pd_list = standardizeColumns(pd_list)
    for i in range(len(pd_list)):
        print 'Saving %s...' % (dataset_ids[i][0])
        name = 'raw_%s.csv' % dataset_ids[i][0]
        pd_list[i].to_csv('%s/%s' % (output_dir, name))
    print  'Downloaded and saved dataframes: %s. Running combine_dataframes()... ' % sum(x.shape[0] for x in pd_list)
    all_patients = combine_dataframes(pd_list)
    print 'Combined dataframes: %s. Running codeMedicare()... ' % sum(x.shape[0] for x in pd_list)
    all_patients = codeMedicare(all_patients)
    print 'Coded medicare: %s. Running adjustForInflation()... ' % all_patients.shape[0]
    all_patients = adjustForInflation(all_patients, 'total_costs')
    all_patients = adjustForInflation(all_patients, 'total_charges')
    print 'Adjusted for inflation: %s. Running assignNumeric()...' % all_patients.shape[0]
    all_patients = assignNumeric(all_patients)

    print 'Keeping %s' % (columns_to_keep)
    all_patients_keep = all_patients[columns_to_keep]


    print 'Assigned numeric: %s. Running subsetMedicare()...' % all_patients.shape[0]
    medicare = subsetMedicare(all_patients)

    ############# medicare made
    print ('Subsetted medicare: all = %s, medicare only = %s. '
           'Only using medicare now. Running additional_cleaning()... ') % (all_patients.shape[0],medicare.shape[0])
    medicare = additional_cleaning(medicare)
    print 'Additional_cleaning: %s. Running removeOutliers()... ' % medicare.shape[0]
    medicare = removeOutliers(medicare)
    print 'removeOutliers - TC/LOS: %s' % medicare.shape[0]

    # subset medicare
    medicare_keep = medicare[columns_to_keep]

    all_out_file = '%s/%s' % (output_dir, 'all_patients.csv')
    all_patients.to_csv(all_out_file, index=False)
    print 'Saved %s' % all_out_file

    all_keep_file = '%s/%s' % (output_dir, 'all_patients_column_subset.csv')
    all_patients_keep.to_csv(all_keep_file, index=False)
    print 'Saved %s' % all_keep_file

    medicare_out_file = '%s/%s' % (output_dir, 'medicare.csv')
    medicare.to_csv(medicare_out_file,index=False)
    print 'Saved %s' % medicare_out_file

    medicare_out_keep_file = '%s/%s' % (output_dir, 'medicare_column_subset.csv')
    medicare_keep.to_csv(medicare_out_keep_file,index=False)
    print 'Saved %s' % medicare_out_keep_file

    print 'DONE'


if __name__ == "__main__":
    app.run(main)
