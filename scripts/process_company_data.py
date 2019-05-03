#!/usr/bin/env

import sys
import pandas as pd
from pandas.io.json import json_normalize
import s3fs
import numpy as np

fs = s3fs.S3FileSystem(
    key=sys.argv[1], secret=sys.argv[2],
    anon=False)  # create AWS S3 filesystem

ROOT_DIR = ''
PSC_FILE_PATH = '{}raw/persons-with-significant-control-snapshot-2019-03-05.txt'.format(
    ROOT_DIR)
LIVE_COMPANIES_PATH = '{}raw/BasicCompanyDataAsOneFile-2019-03-01.csv'.format(
    ROOT_DIR)
OFFICERS_DIRECTORY_PATH = '{}raw/officers/'.format(ROOT_DIR)
DISQUALIFIED_DIRECTORS_PATH = '{}raw/disqualified_directors/'.format(ROOT_DIR)
POLITICIANS_PATH = '{}processed/politicians.csv'.format(ROOT_DIR)
URL_COMPANY_CODES_PATH = '{}interim/companies_house_url_type_codes.csv'.format(
    ROOT_DIR)

try:
    if sys.argv[3] == 'test':
        nrows = int(sys.argv[4])
        PSC_FILE_PATH = '{}raw/psc_sample.txt'.format(ROOT_DIR)
        test_run = True
        print(
            'Running on sample of {} records per file and test JSON...'.format(
                str(nrows)))
    else:
        nrows = None
        print('Running on full data...')
except Exception:
    nrows = None
    test_run = False
    print('Running on full data...')


def main():
    live_companies_list, live_company_map = process_live_companies(
        LIVE_COMPANIES_PATH)
    disqualified_directors = process_disqualified_directors_data(
        DISQUALIFIED_DIRECTORS_PATH)
    politicians = process_politicians(POLITICIANS_PATH)
    process_psc_data(PSC_FILE_PATH, live_companies_list, live_company_map,
                     disqualified_directors, politicians)
    process_officers(OFFICERS_DIRECTORY_PATH, live_companies_list, politicians)
    print('Script finished!')


def process_disqualified_directors_data(disqualfied_directors_path):
    disqualified_directors = read_disqualified_directors(
        DISQUALIFIED_DIRECTORS_PATH)
    disqualified_directors = create_additional_columns_diqual_directors(
        disqualified_directors)
    write_csv_s3(disqualified_directors, 'disqualified_directors', fs)
    return disqualified_directors


def process_politicians(politicians_path):
    df = pd.read_csv(fs.open(politicians_path))
    output_df = df[['join_id', 'leg_country', 'leg_name',
                    'active_periods']].copy()
    output_df.columns = [
        'join_id', 'politician_leg_country', 'politician_leg_name',
        'politician_active_periods'
    ]
    return output_df


def process_psc_data(psc_file_path, live_companies_list, live_company_map,
                     disqualified_directors, politicians):
    url_company_codes, url_company_codes_s = create_url_company_codes(
        URL_COMPANY_CODES_PATH)
    psc_json_path = PSC_FILE_PATH
    all_records = read_psc_json(fs.open(psc_json_path))
    all_records = remove_no_record_rows(all_records)
    all_records.columns = standardise_columns(all_records.columns)
    all_records = create_additional_columns_all_records(
        all_records, live_company_map, disqualified_directors, politicians)
    psc_records = create_records_psc_df(all_records)
    psc_records, exemption_records = split_exemptions_from_psc_records(
        psc_records)
    active_psc_records, ceased_psc_records = split_active_ceased(
        psc_records, live_companies_list)
    active_psc_controls = create_psc_controls_df(active_psc_records)
    active_exemption_records, ceased_exemption_records = split_active_ceased(
        exemption_records, live_companies_list)
    psc_statements = create_psc_statements_df(all_records)
    active_psc_statements, ceased_psc_statements = split_active_ceased(
        psc_statements, live_companies_list)
    write_csv_s3(active_psc_records, 'active_psc_records', fs)
    write_csv_s3(ceased_psc_records, 'ceased_psc_records', fs)
    write_csv_s3(active_psc_statements, 'active_psc_statements', fs)
    write_csv_s3(ceased_psc_statements, 'ceased_psc_statements', fs)
    write_csv_s3(active_exemption_records, 'active_exemption_records', fs)
    write_csv_s3(ceased_exemption_records, 'ceased_exemption_records', fs)
    write_csv_s3(active_psc_controls, 'active_psc_controls', fs)
    print('Processed PSC data')


def process_live_companies(live_companies_path):
    url_company_codes, url_company_codes_s = create_url_company_codes(
        URL_COMPANY_CODES_PATH)
    live_companies = load_live_companies(fs.open(live_companies_path))
    live_companies = clean_live_companies(live_companies)
    live_companies = create_additional_columns_live_companies(live_companies)
    write_csv_s3(live_companies, 'companies', fs)
    live_companies_list = live_companies.company_number.unique().tolist()
    live_company_map = live_companies[[
        'company_number', 'company_name', 'first_and_postcode'
    ]].set_index('company_number')
    print('Processed live companies')
    return live_companies_list, live_company_map


def process_officers(officers_directory_path, live_companies_list,
                     politicians):
    officers_people_files = get_officers_files(fs.ls(officers_directory_path))
    officers = read_officers(officers_directory_path, officers_people_files)
    officers.columns = standardise_columns(officers.columns)
    active_officers = filter_active_officers(officers, live_companies_list)
    active_officers = add_additional_columns_officers(active_officers,
                                                      politicians)
    write_csv_s3(active_officers, 'active_officers', fs)
    print('Processed officers')


def load_live_companies(path):
    output = pd.read_csv(path, low_memory=False, nrows=nrows)
    print('Loaded live companies...')
    return output


def clean_live_companies(df):
    output = df.copy()
    output.columns = [x.strip() for x in output.columns]
    output.columns = standardise_columns(output)
    output.rename(
        columns={
            'companynumber': 'company_number',
            'companyname': 'company_name'
        },
        inplace=True)
    print('Cleaned live companies...')
    return output


def create_additional_columns_live_companies(df):
    url_company_codes, url_company_codes_s = create_url_company_codes(
        URL_COMPANY_CODES_PATH)
    output = df.copy()
    output['first_and_postcode'] = output[
        'regaddress_addressline1'] + '-' + output['regaddress_postcode']
    output['incorporation_date_formatted'] = pd.to_datetime(
        output['incorporationdate'], errors='coerce', format='%d/%m/%Y')
    output['type_codes'] = output.company_number.apply(company_code_creator)
    output['company_type'] = output.type_codes.map(url_company_codes_s)
    output['psc_regime_applies'] = psc_regime_applies(output)
    print('Created additional columns in live companies...')
    return output


def company_code_creator(x):
    if x[:2].isdigit():
        return 'EAW'
    else:
        return x[:2]


def create_url_company_codes(path):
    url_company_codes = pd.read_csv(fs.open(path), keep_default_na=False)
    url_company_codes.columns = standardise_columns(url_company_codes)
    url_company_codes_s = pd.Series(
        url_company_codes['company_type'].values,
        index=url_company_codes.prefix)
    return url_company_codes, url_company_codes_s


def psc_regime_applies(df):
    url_company_codes, url_company_codes_s = create_url_company_codes(
        URL_COMPANY_CODES_PATH)
    excludedcompanytypes = url_company_codes[
        url_company_codes['excluded_from_psc'] == 'X']['prefix'].tolist()
    excludedcompanycategories = [
        'Industrial and Provident Society', 'Registered Society'
    ]
    additional_excluded_company_types = ['CE', 'CS', 'PC']
    excludedcompanytypes.extend(additional_excluded_company_types)
    output_s = ~(df.companycategory.isin(excludedcompanycategories)
                 | df.type_codes.isin(excludedcompanytypes))
    return output_s


def read_disqualified_directors(disqualfied_directors_path):
    disqual_files = fs.ls(disqualfied_directors_path)
    for path in disqual_files:
        if 'disqualifications' in path:
            disquals_df = pd.read_csv(
                fs.open(path), dtype={'person_number': str})
        elif 'persons' in path:
            persons_df = pd.read_csv(
                fs.open(path), dtype={'person_number': str})
        elif 'exemptions' in path:
            exemptions_df = pd.read_csv(
                fs.open(path), dtype={'person_number': str})
    output_df = pd.merge(persons_df, disquals_df, on='person_number')
    return output_df


def create_additional_columns_diqual_directors(df):
    df['person_dob_formatted'] = pd.to_datetime(
        df['person_dob'], format="%Y%m%d", errors='coerce')
    df['persons_month_year'] = df.person_dob_formatted.dt.strftime('%Y-%m')
    df['join_id'] = df[['forenames', 'surname', 'person_dob_formatted']].apply(
        create_join_id,
        first_name_col='forenames',
        surname_col='surname',
        month_year_birth_col='person_dob_formatted', axis=1)
    df['disqual_start_date_formatted'] = pd.to_datetime(
        df['disqual_start_date'], format="%Y%m%d", errors='coerce')
    df['disqual_end_date_formatted'] = pd.to_datetime(
        df['disqual_end_date'], format="%Y%m%d", errors='coerce')
    return df


def read_psc_json(path):
    temp_df = pd.read_json(path, lines=True)
    output_df = pd.concat(
        [temp_df['company_number'],
         json_normalize(temp_df['data'])], axis=1)
    print('Read JSON file...')
    return output_df


def remove_no_record_rows(df):
    # remove last line of DataFrame which is not a record
    output_df = df.iloc[:-1].copy()
    # remove summary totals
    output_df = output_df[
        output_df.kind != 'totals#persons-of-significant-control-snapshot']
    print('Removed extra rows...')
    return output_df


def create_psc_controls_df(df):
    # create a DataFrame of ways of controlling companies
    temp_df = df[['company_number', 'natures_of_control'
                  ]].dropna(subset=['natures_of_control']).copy()
    list_of_lists = []
    for index, row in temp_df.iterrows():
        for item in row['natures_of_control']:
            list_of_lists.append([row['company_number'], item])
    output_df = pd.DataFrame(list_of_lists)
    output_df.columns = ['company_number', 'nature_of_control']
    print('Created PSC controls df...')
    return output_df


def create_additional_columns_all_records(df, live_company_map,
                                          disqualified_directors, politicians):
    url_company_codes, url_company_codes_s = create_url_company_codes(
        URL_COMPANY_CODES_PATH)
    temp_df = df.copy()
    temp_df['month_year_birth'] = temp_df['date_of_birth_year'].dropna(
    ).astype(str).str.replace(
        r'\.0', '') + '-' + temp_df['date_of_birth_month'].dropna().astype(
            str).str.replace(r'\.0', '')
    temp_df['month_year_birth'] = pd.to_datetime(
        temp_df['month_year_birth'], format='%Y-%m', errors='coerce')
    temp_df['join_id'] = temp_df[[
        'name_elements_forename', 'name_elements_surname', 'month_year_birth'
    ]].apply(
        create_join_id,
        first_name_col='name_elements_forename',
        surname_col='name_elements_surname',
        month_year_birth_col='month_year_birth', axis=1)
    temp_df['type_codes'] = temp_df.company_number.apply(company_code_creator)
    temp_df['company_type'] = temp_df.type_codes.map(url_company_codes_s)
    temp_df['address_country_normal'] = temp_df['address_country'].str.upper()
    temp_df['address_country_normal'].fillna('', inplace=True)
    temp_df['registered_country_normal'] = df[
        'identification_country_registered'].str.upper()
    temp_df['registered_country_normal'].fillna('', inplace=True)
    temp_df['country_of_residence_normal'] = temp_df[
        'country_of_residence'].str.upper()
    temp_df['country_of_residence_normal'].fillna('', inplace=True)
    temp_df['registered_country_normal'] = clean_countries(
        temp_df['registered_country_normal'])
    temp_df['address_country_normal'] = clean_countries(
        temp_df['address_country_normal'])
    temp_df['possible_politician'] = temp_df.join_id.isin(
        politicians.join_id.dropna().unique())
    temp_df = pd.merge(temp_df, politicians, on='join_id', how='left')
    print('Added politician field and associated data...')
    secret_jurisdictions = create_secrecy_jurisdiction_list(
        fs.open('{}interim/secret_jurisdictions.csv'.format(ROOT_DIR)))
    temp_df['secret_base'] = temp_df.apply(
        secret_function, secret_jurisdictions=secret_jurisdictions, axis=1)
    print('Added secret base field...')
    rle_list = create_rle_list(
        fs.open('{}interim/recognised_stock_exchange_countries.csv'.format(
            ROOT_DIR)))
    temp_df['non_rle_country'] = temp_df.apply(
        non_rle_function, rle_list=rle_list, axis=1)
    print('Added non-rle country field...')
    temp_df['psc_likely_disqualified_director'] = temp_df.join_id.isin(
        disqualified_directors.dropna(
            subset=['persons_month_year']).join_id.dropna().unique())
    temp_df['company_name'] = temp_df.company_number.map(
        live_company_map['company_name'])
    temp_df['company_first_and_postcode'] = temp_df.company_number.map(
        live_company_map['first_and_postcode'])
    print('Created additional columns on all records df...')
    return temp_df


def create_records_psc_df(df):
    if 'statement' in df.columns:
        output_df = df[pd.isnull(df.statement)].copy()
        print('Created records df...')
        return output_df
    else:
        print('No statements to create df for')
        return df


def split_active_ceased(df, live_companies_list):
    if df is None:
        print('Nothing to split as df empty...')
        return None, None
    else:
        active = df[pd.isnull(df.ceased_on)].copy()
        active = active[active.company_number.isin(live_companies_list)]
        ceased = df[~df.company_number.isin(active.company_number)]
        return active, ceased


def create_psc_statements_df(df):
    if 'statement' in df.columns:
        output_df = df[~pd.isnull(df.statement)].copy()
        print('Created statements df...')
        return output_df
    else:
        return None


def split_exemptions_from_psc_records(df):
    exemption_records = df[df.kind == 'exemptions']
    active_psc_records = df[df.kind != 'exemptions']
    return active_psc_records, exemption_records


def clean_countries(s):
    temp_s = s.copy()
    registered_country_clean_map = pd.read_csv(
        fs.open(
            '{}interim/registered_country_cleaner_map.csv'.format(ROOT_DIR)))
    address_country_clean_map = pd.read_csv(
        fs.open('{}interim/address_country_cleaner_map.csv'.format(ROOT_DIR)))
    combined_clean_map = pd.concat(
        [registered_country_clean_map, address_country_clean_map])
    combined_clean_map.drop_duplicates(subset=['original'], inplace=True)
    combined_clean_map_s = pd.Series(
        combined_clean_map.clean.values, index=combined_clean_map.original)
    output_s = temp_s.map(combined_clean_map_s)
    print('Cleaned country fields...')
    return output_s


def create_rle_list(path):
    temp_df = pd.read_csv(path)
    output = temp_df['country_name'].str.upper().tolist()
    output.extend([
        'ENGLAND', 'SCOTLAND', 'NORTHERN IRELAND', 'GREAT BRITAIN', 'UK',
        'WALES', 'UNITED STATES OF AMERICA', 'UNITED STATES',
        'ENGLAND & WALES', 'REPUBLIC OF IRELAND', 'IRELAND',
        'ENGLAND AND WALES'
    ])
    return output


def secret_function(x, secret_jurisdictions):
    if x['country_of_residence_normal'] in secret_jurisdictions:
        return True
    elif x['address_country_normal'] in secret_jurisdictions:
        return True
    elif x['registered_country_normal'] in secret_jurisdictions:
        return True
    else:
        return False


def non_rle_function(x, rle_list):
    if x['kind'] == 'corporate-entity-person-with-significant-control' and ~pd.isnull(
            x['registered_country_normal']
    ) and x['address_country_normal'] not in rle_list and x[
            'registered_country_normal'] not in rle_list:
        return True
    else:
        return False


def get_officers_files(officer_files):
    output = [x for x in officer_files
              if 'persons_data' in x]  # filter only for officer person files
    print('Grabbed raw officer paths...')
    return output


def read_officers(directory, officers_files):
    output_df = pd.DataFrame()
    for file in officers_files:
        temp_df = pd.read_csv(
            fs.open(file),
            dtype={
                'Company Number': str,
                'Person number': str,
                'Partial Date of Birth': str,
            },
            low_memory=False,
            nrows=nrows)
        output_df = pd.concat([temp_df, output_df])
    output_df.reset_index(inplace=True)
    print('Combined officers into a single df...')
    return output_df


def clean_officers(df):
    temp_df = df.copy()
    return temp_df


def filter_active_officers(df, live_companies_list):
    temp_df = df[df.company_number.isin(live_companies_list)]
    return temp_df


def add_additional_columns_officers(df, politicians):
    temp_df = df.copy()
    temp_df['partial_date_of_birth_formatted'] = pd.to_datetime(
        temp_df.partial_date_of_birth.astype(str).str.strip() + '01',
        format='%Y%m%d',
        errors='coerce')
    temp_df['appointment_date_formatted'] = pd.to_datetime(
        temp_df.appointment_date.astype(str).str.strip(),
        format='%Y%m%d',
        errors='coerce')
    temp_df['country_of_residence_normal'] = temp_df[
        'resident_country'].str.upper()
    temp_df['address_country_normal'] = temp_df['country'].str.upper()
    secret_jurisdictions = create_secrecy_jurisdiction_list(
        fs.open('{}interim/secret_jurisdictions.csv'.format(ROOT_DIR)))
    temp_df['secret_base'] = temp_df.apply(
        secret_officer_function,
        secret_jurisdictions=secret_jurisdictions,
        axis=1)
    temp_df['join_id'] = temp_df[[
        'forenames', 'surname', 'partial_date_of_birth_formatted'
    ]].apply(
        create_join_id,
        first_name_col='forenames',
        surname_col='surname',
        month_year_birth_col='partial_date_of_birth_formatted', axis=1)
    temp_df['possible_politician'] = temp_df.join_id.isin(
        politicians.join_id.unique())
    temp_df = pd.merge(temp_df, politicians, on='join_id', how='left')
    print('Added politician field and associated data...')
    appointment_type_label_dict = {
        0: 'Current Secretary',
        1: 'Current Director',
        4: 'Current non-designated LLP Member',
        5: 'Current designated LLP Member',
        11: 'Current Judicial Factor',
        12: 'Current Receiver or Manager appointed under the Charities Act',
        13: 'Current Manager appointed under the CAICE Act',
        17: 'Current SE Member of Administrative Organ',
        18: 'Current SE Member of Supervisory Organ',
        19: 'Current SE Member of Management Organ'
    }
    temp_df['appointment_type_label'] = temp_df.appointment_type.map(
        appointment_type_label_dict)
    print('Created additional officers columns...')
    return temp_df


def standardise_columns(columns):
    output = [x.replace(' ', '_') for x in columns]
    output = [x.lower() for x in output]
    output = [x.replace('.', '_') for x in output]
    print('Standardized df columns...')
    return output


def create_secrecy_jurisdiction_list(path):
    temp_df = pd.read_csv(path, header=None)[0]
    output = temp_df.tolist()
    return output


def secret_officer_function(x, secret_jurisdictions):
    if x['country_of_residence_normal'] in secret_jurisdictions:
        return True
    elif x['address_country_normal'] in secret_jurisdictions:
        return True
    else:
        return False


def create_join_id(x, first_name_col, surname_col, month_year_birth_col):
    if not x.isnull().values.any():
        first_name = x[first_name_col].split(' ')[0]
        month_year = x[month_year_birth_col].strftime('%Y-%m')
        join_id = first_name + '-' + x[surname_col] + '_' + month_year
        join_id = join_id.strip('-_ ')
        join_id = join_id.upper()
        join_id = join_id.replace(' ', '')
        return join_id
    else:
        return np.nan


def write_csv_s3(df, filename, fs):
    if df is None:
        print('Empty df, no CSV for {} written...'.format(filename))
    else:
        if test_run:
            with fs.open('{}test-output/{}.csv'.format(ROOT_DIR, filename),
                         'w') as f:
                df.to_csv(f, chunksize=100000, index=False)
        else:
            with fs.open('{}processed/{}.csv'.format(ROOT_DIR, filename),
                         'w') as f:
                df.to_csv(f, chunksize=100000, index=False)
        print('Wrote {} to CSV'.format(filename))


if __name__ == '__main__':
    main()
