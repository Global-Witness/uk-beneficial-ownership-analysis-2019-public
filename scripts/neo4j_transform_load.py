#!/usr/bin/env
from py2neo import Graph, Schema
import pandas as pd
import sys
import s3fs
import numpy as np
import recordlinkage
import re

fs = s3fs.S3FileSystem(
    key=sys.argv[1], secret=sys.argv[2],
    anon=False)  # create AWS S3 filesystem

graph = Graph(
    NEO4J_URL,
    auth=(sys.argv[3], sys.argv[4]),
    secure=True)

ROOT_DIR_INPUT = ''
ROOT_DIR_OUTPUT = ''
S3_BASE = ''

try:
    if sys.argv[5] == 'test':
        nrows = int(sys.argv[6])
        print('Running on sample of {} records...'.format(str(nrows)))
    else:
        nrows = None
        print('Running on full data...')
except Exception:
    nrows = None
    print('Running on full data...')

live_companies = pd.read_csv(
    fs.open('{}processed/companies.csv'.format(ROOT_DIR_INPUT)),
    parse_dates=['incorporation_date_formatted'],
    low_memory=False,
    nrows=nrows)
active_psc_records = pd.read_csv(
    fs.open('{}processed/active_psc_records.csv'.format(ROOT_DIR_INPUT)),
    parse_dates=['month_year_birth'],
    low_memory=False,
    nrows=nrows)
active_psc_statements = pd.read_csv(
    fs.open('{}processed/active_psc_statements.csv'.format(ROOT_DIR_INPUT)),
    low_memory=False,
    nrows=nrows)
active_psc_controls = pd.read_csv(
    fs.open('{}processed/active_psc_controls.csv'.format(ROOT_DIR_INPUT)),
    low_memory=False,
    nrows=nrows)
active_exemptions = pd.read_csv(
    fs.open('{}processed/active_exemption_records.csv'.format(ROOT_DIR_INPUT)),
    low_memory=False,
    nrows=nrows)
ceased_exemptions = pd.read_csv(
    fs.open('{}processed/ceased_exemption_records.csv'.format(ROOT_DIR_INPUT)),
    low_memory=False,
    nrows=nrows)
active_officers = pd.read_csv(
    fs.open('{}processed/active_officers.csv'.format(ROOT_DIR_INPUT)),
    low_memory=False,
    nrows=nrows,
    parse_dates=[
        'partial_date_of_birth_formatted', 'appointment_date_formatted'
    ],
    dtype={'person_number': str})

csv_file_records = [
]  # list to store information on CSV file for Neo4J import queries


def main():
    active_filing_company_nodes = prepare_filing_company_data(
        active_psc_records, active_psc_statements, active_exemptions,
        live_companies)
    active_target_company_nodes = prepare_target_company_data(
        active_psc_records)
    active_officers_company_nodes = prepare_company_officer_data(
        active_officers, live_companies)
    combine_company_nodes(active_filing_company_nodes,
                          active_target_company_nodes,
                          active_officers_company_nodes)
    active_officer_human_nodes = prepare_human_officer_data(active_officers)
    active_psc_human_nodes = prepare_human_psc_data(active_psc_records)
    combine_person_nodes(active_officer_human_nodes, active_psc_human_nodes)
    prepare_legal_person_psc_data(active_psc_records)
    prepare_psc_exemptions_data(active_exemptions)
    prepare_psc_statements_data(active_psc_statements)
    prepare_super_secure_data(active_psc_records)
    prepare_address_data(live_companies)
    print(csv_file_records)
    clear_graph(graph)
    create_constraints([
        'Person', 'Company', 'Exemption', 'Statement', 'SuperSecure',
        'Postcode', 'LegalPerson'
    ], graph)
    node_csvs = get_node_csvs(csv_file_records)
    create_all_nodes(node_csvs, graph)
    edge_csvs = get_edge_csvs(csv_file_records)
    create_all_edges(edge_csvs, graph)
    print('Script finished!')


def combine_company_nodes(filing_company_nodes, target_company_nodes,
                          active_officers_company_nodes):
    company_nodes = pd.concat([
        filing_company_nodes, target_company_nodes,
        active_officers_company_nodes
    ],
                              axis=0,
                              ignore_index=True,
                              sort=True)
    company_nodes.drop_duplicates(inplace=True)
    company_nodes['full_address'] = company_nodes[[
        'address_line_1', 'address_line_2', 'county', 'country', 'town',
        'postcode'
    ]].fillna('').apply(
        ','.join, axis=1)
    company_nodes['full_address'] = company_nodes['full_address'].str.replace(
        r'(,\s*){1,}', ', ')
    company_nodes['full_address'] = company_nodes['full_address'].str.strip(
        ', ')
    company_nodes.dropna(subset=['uid'], inplace=True)
    company_nodes['name'] = company_nodes['name'].str.upper()
    company_nodes = company_nodes[[
        'uid', 'company_number', 'full_address', 'country_of_origin',
        'country_registered', 'dissolution_date', 'exemptions_count',
        'incorporation_date', 'legal_authority', 'legal_form',
        'name', 'place_registered', 'resident_country',
        'country_of_residence_normal', 'address_country_normal', 'secret_base'
    ]].fillna('').astype(str).groupby(
        'uid').agg(lambda x: ' | '.join(list(set(x)))).reset_index()
    company_nodes = company_nodes.apply(lambda x: x.str.strip('| '))
    company_nodes = company_nodes.apply(lambda x: x.str.upper())
    perform_unique_check(company_nodes, 'uid')
    filename = 'company_nodes'
    create_file_record(filename, 'nodes', label='Company')
    write_csv_s3_neo(company_nodes, filename, fs)


def combine_person_nodes(human_officer_nodes, human_psc_nodes):
    person_nodes = pd.concat([human_officer_nodes, human_psc_nodes],
                             axis=0,
                             ignore_index=True,
                             sort=True)
    person_nodes.drop_duplicates(inplace=True)
    person_nodes['full_address'] = person_nodes[[
        'address_line_1', 'address_line_2', 'care_of', 'po_box', 'county',
        'locality', 'country', 'town', 'postcode'
    ]].fillna('').apply(
        ', '.join, axis=1)
    person_nodes['full_address'] = person_nodes['full_address'].str.replace(
        r'(,\s*){1,}', ', ')
    person_nodes['full_address'] = person_nodes['full_address'].str.strip(', ')
    list_of_titles = [
        'MR', 'MRS', 'DR', 'MISS', 'SIR', 'PROFESSOR', 'PROF', 'LORD', 'LADY',
        'RT HON', 'DOCTOR', 'DR', 'ESQ', 'DAME', 'MX'
    ]
    person_nodes['name'] = person_nodes['name'].str.replace(
        r'^({})\.?\s'.format('|'.join(list_of_titles)), '')
    person_nodes = person_nodes[[
        'uid', 'name', 'title', 'honours', 'full_address', 'nationality',
        'month_year_birth', 'country_of_residence_normal',
        'address_country_normal', 'secret_base', 'join_id',
        'psc_likely_disqualified_director', 'possible_politician',
        'politician_leg_country', 'politician_leg_name',
        'politician_active_periods'
    ]].fillna('').astype(str).groupby(
        'uid').agg(lambda x: ' | '.join(list(set(x)))).reset_index()
    person_nodes['join_id'] = person_nodes['join_id'].str.split(
        ' | ').apply(lambda x: x[0])
    person_nodes = person_nodes.apply(lambda x: x.str.strip('| '))
    person_nodes = person_nodes.apply(lambda x: x.str.upper())
    person_nodes = person_nodes.apply(lambda x: x.str.replace(
        r'(\\)$', ''))  # neo was assuming excaped speechmarks in one place
    create_probable_same_person_edges(person_nodes)
    perform_unique_check(person_nodes, 'uid')
    filename = 'person_nodes'
    create_file_record(filename, 'nodes', label='Person')
    write_csv_s3_neo(person_nodes, filename, fs)


def prepare_filing_company_data(active_psc_records, active_psc_statements,
                                active_exemptions, live_companies):
    temp_1 = pd.merge(
        live_companies,
        active_psc_records.drop('company_name', axis=1),
        on='company_number',
        how='outer')  # include all live compamies
    temp_2 = pd.merge(
        live_companies,
        active_psc_statements.drop('company_name', axis=1),
        on='company_number',
        how='outer')
    temp_3 = pd.merge(
        live_companies,
        active_exemptions.drop('company_name', axis=1),
        on='company_number',
        how='outer')
    active_filing_company_psc = pd.concat([temp_1, temp_2, temp_3], sort=False)
    active_filing_company_psc.drop_duplicates(inplace=True)
    active_filing_company_psc['uid'] = active_filing_company_psc.company_number
    active_filing_company_psc.fillna('', inplace=True)
    active_filing_company_nodes = create_filing_company_psc_nodes(
        active_filing_company_psc)
    return active_filing_company_nodes


def prepare_target_company_data(active_psc_records):
    active_target_company_psc = active_psc_records[
        active_psc_records.kind ==
        'corporate-entity-person-with-significant-control'].copy()
    active_target_company_psc.fillna('', inplace=True)
    active_target_company_psc['uid'] = active_target_company_psc.apply(
        create_target_company_uid, axis=1)
    active_target_company_psc = active_target_company_psc[[
        'uid', 'company_number', 'address_address_line_1',
        'address_address_line_2', 'address_country', 'address_postal_code',
        'exemptions_count', 'notified_on',
        'identification_country_registered', 'identification_legal_authority',
        'identification_legal_form', 'identification_place_registered',
        'identification_registration_number', 'name', 'natures_of_control',
        'secret_base'
    ]].copy()
    active_target_company_nodes = create_active_target_company_psc_nodes(
        active_target_company_psc)
    create_company_edges(active_target_company_psc)
    return active_target_company_nodes


def prepare_human_psc_data(active_psc_records):
    active_human_psc = active_psc_records[
        active_psc_records.kind ==
        'individual-person-with-significant-control'].copy()
    active_human_psc['uid'] = active_human_psc[[
        'name_elements_forename', 'name_elements_surname', 'month_year_birth',
        'address_postal_code', 'etag'
    ]].apply(
        create_person_uid,
        first_name_col='name_elements_forename',
        surname_col='name_elements_surname',
        month_year_birth_col='month_year_birth',
        post_code_col='address_postal_code',
        backup_id_col='etag',
        axis=1)
    active_human_psc.fillna('', inplace=True)
    active_human_psc_nodes = create_active_human_psc_nodes(active_human_psc)
    create_human_edges(active_human_psc)
    return active_human_psc_nodes


def prepare_legal_person_psc_data(active_psc_records):
    active_legal_psc = active_psc_records[
        active_psc_records.kind ==
        'legal-person-person-with-significant-control'].copy()
    active_legal_psc['uid'] = active_legal_psc[
        'name'] + '-' + active_legal_psc['address_postal_code'].fillna('')
    active_legal_psc['uid'] = active_legal_psc['uid'].str.upper()
    active_legal_psc['uid'] = active_legal_psc['uid'].str.strip('_- ')
    active_legal_psc.fillna('', inplace=True)
    create_legal_person_psc_nodes(active_legal_psc)
    create_legal_person_psc_edges(active_legal_psc)


def prepare_psc_exemptions_data(active_exmemptions):
    active_exemptions_psc = active_psc_records[active_psc_records.kind ==
                                               'exemptions'].copy()
    active_exemptions_psc['uid'] = active_exemptions_psc['etag'].copy()
    active_exemptions_psc.fillna('', inplace=True)
    create_exemption_nodes(active_exemptions_psc)
    create_exemption_edges(active_exemptions_psc)


def prepare_psc_statements_data(active_psc_statements):
    active_psc_statements['uid'] = active_psc_statements['etag']
    active_psc_statements.drop_duplicates(inplace=True)
    create_psc_statement_nodes(active_psc_statements)
    create_statement_edges(active_psc_statements)


def prepare_super_secure_data(active_psc_records):
    active_super_secure_psc = active_psc_records[
        active_psc_records.kind ==
        'super-secure-person-with-significant-control'].copy()
    active_super_secure_psc['uid'] = active_super_secure_psc['etag'].copy()
    active_super_secure_psc.fillna('', inplace=True)
    create_super_secure_nodes(active_super_secure_psc)
    create_super_secure_edges(active_super_secure_psc)


def prepare_address_data(live_companies):
    active_addresses = live_companies.melt(
        id_vars=['regaddress_postcode'], value_vars=['company_number'])
    active_addresses['uid'] = active_addresses['regaddress_postcode'].copy()
    active_addresses.dropna(subset=['uid'], inplace=True)
    create_address_nodes(active_addresses)
    create_address_edges(active_addresses)


def prepare_human_officer_data(active_officers):
    active_officers_humans = active_officers[
        active_officers.corporate_indicator != 'Y'].copy()
    active_officers_humans['uid'] = active_officers[[
        'forenames', 'surname', 'partial_date_of_birth_formatted',
        'person_postcode', 'person_number'
    ]].apply(
        create_person_uid,
        first_name_col='forenames',
        surname_col='surname',
        month_year_birth_col='partial_date_of_birth_formatted',
        post_code_col='person_postcode',
        backup_id_col='person_number',
        axis=1)
    active_officers_humans_nodes = create_human_officer_nodes(
        active_officers_humans)
    create_human_officers_edges(active_officers_humans)
    return active_officers_humans_nodes


def prepare_company_officer_data(active_officers, live_companies):
    active_officers_companies = active_officers[
        active_officers.corporate_indicator == 'Y'].copy()
    active_officers_companies.fillna('', inplace=True)
    active_officers_companies.rename(columns={'surname': 'name'}, inplace=True)
    live_companies_copy = live_companies[[
        'company_number', 'company_name', 'regaddress_postcode'
    ]].copy()
    live_companies_copy['fake_id'] = live_companies_copy.apply(
        create_live_fake_id, axis=1)
    live_companies_copy.dropna(subset=['fake_id'], inplace=True)
    fake_id_map = pd.Series(
        live_companies_copy['company_number'].values,
        index=live_companies_copy['fake_id'])
    fake_id_map = fake_id_map.drop_duplicates()
    fake_id_map = fake_id_map.to_dict()
    active_officers_companies[
        'name_normal'] = active_officers_companies.name.apply(
            lambda x: normalize_company_name(x))
    active_officers_companies['fake_id'] = active_officers_companies[
        'name_normal'] + '_' + active_officers_companies['person_postcode']
    active_officers_companies['uid'] = active_officers_companies.apply(
        lambda x: fake_id_map[x['fake_id']]
        if x['fake_id'] in fake_id_map.keys() else x['person_number'],
        axis=1)
    active_officers_companies_nodes = create_company_officer_nodes(
        active_officers_companies)
    create_company_officers_edges(active_officers_companies)
    return active_officers_companies_nodes


def create_filing_company_psc_nodes(active_filing_company_psc):
    active_filing_psc_columns = [
        'uid', 'company_number', 'regaddress_addressline1',
        'regaddress_addressline2', 'regaddress_posttown', 'regaddress_county',
        'regaddress_country', 'regaddress_postcode', 'companycategory',
        'countryoforigin', 'dissolutiondate', 'incorporationdate',
        'company_name'
    ]
    active_filing_company_psc_nodes = active_filing_company_psc[
        active_filing_psc_columns].drop_duplicates()
    active_filing_company_psc_nodes.columns = [
        'uid', 'company_number', 'address_line_1', 'address_line_2', 'town',
        'county', 'country', 'postcode', 'company_category',
        'country_of_origin', 'dissolution_date', 'incorporation_date', 'name'
    ]
    return active_filing_company_psc_nodes


def create_active_target_company_psc_nodes(active_target_company_psc):
    active_target_company_psc_nodes = active_target_company_psc.copy()
    active_target_company_psc_nodes.columns = [
        'uid', 'company_number_of_filing_company', 'address_line_1',
        'address_line_2', 'country', 'postcode', 'exemptions_count',
        'notified_on', 'country_registered', 'legal_authority', 'legal_form',
        'place_registered', 'company_number', 'name', 'natures_of_control',
        'secret_base'
    ]
    active_target_company_psc_nodes.drop(
        columns=['natures_of_control', 'notified_on'], inplace=True)
    active_target_company_psc_nodes = active_target_company_psc_nodes.drop_duplicates(
    )
    active_target_company_psc_nodes[
        'company_number'] = active_target_company_psc_nodes[
            'company_number'].str.zfill(8)
    return active_target_company_psc_nodes


def create_active_human_psc_nodes(active_human_psc):
    active_human_psc_nodes = active_human_psc[[
        'uid', 'name', 'address_address_line_1', 'address_address_line_2',
        'address_care_of', 'address_country', 'address_locality',
        'address_po_box', 'address_postal_code', 'nationality',
        'month_year_birth', 'country_of_residence_normal',
        'address_country_normal', 'secret_base', 'join_id',
        'psc_likely_disqualified_director', 'possible_politician',
        'politician_leg_country', 'politician_leg_name',
        'politician_active_periods'
    ]].copy()
    active_human_psc_nodes.drop_duplicates(inplace=True)
    active_human_psc_nodes.columns = [
        'uid', 'name', 'address_line_1', 'address_line_2', 'care_of',
        'country', 'locality', 'po_box', 'post_code', 'nationality',
        'month_year_birth', 'country_of_residence_normal',
        'address_country_normal', 'secret_base', 'join_id',
        'psc_likely_disqualified_director', 'possible_politician',
        'politician_leg_country', 'politician_leg_name',
        'politician_active_periods'
    ]
    active_human_psc_nodes.rename(
        columns={'post_code': 'postcode'}, inplace=True)
    active_human_psc_nodes['nationality'] = active_human_psc_nodes[
        'nationality'].str.upper()
    active_human_psc_nodes['name'] = active_human_psc_nodes['name'].str.upper()
    return active_human_psc_nodes


def create_legal_person_psc_nodes(active_legal_psc):
    active_legal_psc_nodes = active_legal_psc[[
        'name', 'address_address_line_1', 'address_address_line_2',
        'address_care_of', 'address_country', 'address_locality',
        'address_po_box', 'address_postal_code', 'nationality',
        'month_year_birth', 'country_of_residence_normal',
        'address_country_normal', 'uid'
    ]].copy()
    active_legal_psc_nodes.drop_duplicates(subset=['uid'], inplace=True)
    active_legal_psc_nodes.columns = [
        'name', 'address_line_1', 'address_line_2', 'care_of', 'country',
        'address_locality', 'po_box', 'post_code', 'nationality',
        'month_year_birth', 'country_of_residence_normal',
        'address_country_normal', 'uid'
    ]
    active_legal_psc_nodes.fillna('', inplace=True)
    active_legal_psc_nodes = active_legal_psc_nodes.apply(lambda x: x.str.upper())
    perform_unique_check(active_legal_psc_nodes, 'uid')
    filename = 'active_legal_psc_nodes'
    create_file_record(filename, 'nodes', label='LegalPerson')
    write_csv_s3_neo(active_legal_psc_nodes, filename, fs)


def create_super_secure_nodes(active_super_secure_psc):
    active_super_secure_psc_nodes = active_super_secure_psc[['uid']].copy()
    active_super_secure_psc_nodes.drop_duplicates(inplace=True)
    filename = 'active_super_secure_psc_nodes'
    perform_unique_check(active_super_secure_psc_nodes, 'uid')
    create_file_record(filename, 'nodes', label='SuperSecure')
    write_csv_s3_neo(active_super_secure_psc_nodes, filename, fs)


def create_exemption_nodes(active_exemptions_psc):
    active_exemptions_psc_nodes = active_exemptions_psc[['uid'
                                                         ]].drop_duplicates()
    perform_unique_check(active_exemptions_psc_nodes, 'uid')
    filename = 'active_exemptions_psc_nodes'
    create_file_record(filename, 'nodes', label='Exemption')
    write_csv_s3_neo(active_exemptions_psc_nodes, filename, fs)


def create_psc_statement_nodes(active_psc_statements):
    active_psc_statements_nodes = active_psc_statements[['statement', 'uid'
                                                         ]].drop_duplicates()
    perform_unique_check(active_psc_statements_nodes, 'uid')
    filename = 'active_psc_statements_nodes'
    create_file_record(filename, 'nodes', label='Statement')
    write_csv_s3_neo(active_psc_statements_nodes, filename, fs)


def create_address_nodes(active_addresses):
    active_address_nodes = active_addresses[['regaddress_postcode',
                                             'uid']].copy()
    active_address_nodes.columns = ['postcode', 'uid']
    active_address_nodes.drop_duplicates(inplace=True)
    perform_unique_check(active_address_nodes, 'uid')
    filename = 'active_address_nodes'
    create_file_record(filename, 'nodes', label='Postcode')
    write_csv_s3_neo(active_address_nodes, filename, fs)


def create_human_officer_nodes(active_officers_humans):
    active_officers_humans_nodes = active_officers_humans[[
        'forenames', 'surname', 'title', 'honours', 'person_number',
        'occupation', 'nationality', 'resident_country',
        'partial_date_of_birth', 'address_line_1', 'address_line_2',
        'post_town', 'county', 'country', 'person_postcode',
        'country_of_residence_normal', 'address_country_normal', 'secret_base',
        'uid', 'join_id', 'possible_politician', 'politician_leg_country',
        'politician_leg_name', 'politician_active_periods'
    ]].copy()
    active_officers_humans_nodes.columns = [
        x.replace('.', '_') for x in active_officers_humans_nodes.columns
    ]
    active_officers_humans_nodes[
        'name'] = active_officers_humans_nodes['forenames'].fillna(
            '') + ' ' + active_officers_humans_nodes['surname'].fillna('')
    active_officers_humans_nodes['name'] = active_officers_humans_nodes[
        'name'].str.strip()
    active_officers_humans_nodes.drop(
        columns=['forenames', 'surname'], inplace=True)
    active_officers_humans_nodes.rename(
        columns={
            'person_postcode': 'postcode',
            'post_town': 'town',
            'partial_date_of_birth_formatted': 'month_year_birth'
        },
        inplace=True)
    return active_officers_humans_nodes


def create_company_officer_nodes(active_officers_companies):
    active_officers_companies_nodes = active_officers_companies[[
        'name', 'nationality', 'resident_country', 'appointment_type_label',
        'address_line_1', 'address_line_2', 'post_town', 'county', 'country',
        'person_postcode', 'country_of_residence_normal',
        'address_country_normal', 'secret_base', 'uid'
    ]].copy()
    active_officers_companies_nodes.rename(
        columns={
            'post_town': 'town',
            'person_postcode': 'postcode'
        },
        inplace=True)
    return active_officers_companies_nodes


def create_human_edges(active_human_psc):
    human_edges = active_human_psc[[
        'company_number', 'uid', 'natures_of_control', 'notified_on'
    ]].copy()
    human_edges.fillna('', inplace=True)
    human_edges.drop_duplicates(subset=['company_number', 'uid'], inplace=True)
    filename = 'psc_human_edges'
    create_file_record(
        filename,
        'edges',
        source={
            'label': 'Person',
            'csv_attribute': 'uid',
            'neo_attribute': 'uid'
        },
        target={
            'label': 'Company',
            'csv_attribute': 'company_number',
            'neo_attribute': 'uid'
        },
        directional=True,
        attributes=['natures_of_control', 'notified_on'],
        relationship_label='CONTROLS')
    write_csv_s3_neo(human_edges, filename, fs)


def create_company_edges(active_target_company_psc):
    company_edges = active_target_company_psc[[
        'company_number', 'uid', 'natures_of_control', 'notified_on'
    ]].copy()
    company_edges.fillna('', inplace=True)
    company_edges.drop_duplicates(
        subset=['company_number', 'uid'], inplace=True)
    filename = 'psc_company_edges'
    create_file_record(
        filename,
        'edges',
        source={
            'label': 'Company',
            'csv_attribute': 'uid',
            'neo_attribute': 'uid'
        },
        target={
            'label': 'Company',
            'csv_attribute': 'company_number',
            'neo_attribute': 'uid'
        },
        attributes=['natures_of_control', 'notified_on'],
        directional=True,
        relationship_label='CONTROLS')
    write_csv_s3_neo(company_edges, filename, fs)


def create_super_secure_edges(active_super_secure_psc):
    super_secure_edges = active_super_secure_psc[[
        'company_number', 'uid', 'natures_of_control', 'notified_on'
    ]].copy()
    super_secure_edges.fillna('', inplace=True)
    super_secure_edges.drop_duplicates(
        subset=['company_number', 'uid'], inplace=True)
    filename = 'super_secure_edges'
    create_file_record(
        filename,
        'edges',
        source={
            'label': 'SuperSecure',
            'csv_attribute': 'uid',
            'neo_attribute': 'uid'
        },
        target={
            'label': 'Company',
            'csv_attribute': 'company_number',
            'neo_attribute': 'uid'
        },
        attributes=['natures_of_control', 'notified_on'],
        directional=True,
        relationship_label='CONTROLS')
    write_csv_s3_neo(super_secure_edges, filename, fs)


def create_legal_person_psc_edges(active_legal_psc):
    legal_person_edges = active_legal_psc[[
        'company_number', 'uid', 'natures_of_control', 'notified_on'
    ]].copy()
    legal_person_edges.fillna('', inplace=True)
    legal_person_edges.drop_duplicates(
        subset=['company_number', 'uid'], inplace=True)
    filename = 'legal_person_edges'
    create_file_record(
        filename,
        'edges',
        source={
            'label': 'LegalPerson',
            'csv_attribute': 'uid',
            'neo_attribute': 'uid'
        },
        target={
            'label': 'Company',
            'csv_attribute': 'company_number',
            'neo_attribute': 'uid'
        },
        attributes=['natures_of_control', 'notified_on'],
        directional=True,
        relationship_label='CONTROLS')
    write_csv_s3_neo(legal_person_edges, filename, fs)


def create_exemption_edges(active_exemptions_psc):
    exemptions_edges = active_exemptions_psc[[
        'company_number', 'uid', 'natures_of_control'
    ]].copy()
    exemptions_edges.fillna('', inplace=True)
    exemptions_edges.drop_duplicates(
        subset=['company_number', 'uid'], inplace=True)
    filename = 'exemption_edges'
    create_file_record(
        filename,
        'edges',
        source={
            'label': 'Company',
            'csv_attribute': 'company_number',
            'neo_attribute': 'uid'
        },
        target={
            'label': 'Exemption',
            'csv_attribute': 'uid',
            'neo_attribute': 'uid'
        },
        attributes=['notified_on'],
        directional=True,
        relationship_label='EXEMPT')
    write_csv_s3_neo(exemptions_edges, filename, fs)


def create_statement_edges(active_psc_statements):
    statement_edges = active_psc_statements[['company_number', 'uid', 'notified_on']].copy()
    statement_edges.fillna('', inplace=True)
    statement_edges.drop_duplicates(
        subset=['company_number', 'uid'], inplace=True)
    filename = 'statement_edges'
    create_file_record(
        filename,
        'edges',
        source={
            'label': 'Company',
            'csv_attribute': 'company_number',
            'neo_attribute': 'uid'
        },
        target={
            'label': 'Statement',
            'csv_attribute': 'uid',
            'neo_attribute': 'uid'
        },
        attributes=['notified_on'],
        directional=True,
        relationship_label='STATES')
    write_csv_s3_neo(statement_edges, filename, fs)


def create_address_edges(active_addresses):
    active_addresses_edges = active_addresses[['value',
                                               'uid']].drop_duplicates()
    filename = 'address_edges'
    create_file_record(
        filename,
        'edges',
        source={
            'label': 'Company',
            'csv_attribute': 'value',
            'neo_attribute': 'uid'
        },
        target={
            'label': 'Postcode',
            'csv_attribute': 'uid',
            'neo_attribute': 'uid'
        },
        directional=True,
        relationship_label='ADDRESS')
    write_csv_s3_neo(active_addresses_edges, filename, fs)


def create_human_officers_edges(active_officers_humans):
    active_officers_humans_edges = active_officers_humans[[
        'uid', 'company_number', 'appointment_type_label',
        'appointment_date_formatted'
    ]].copy()
    active_officers_humans_edges.drop_duplicates(inplace=True)
    filename = 'active_officers_human_edges'
    create_file_record(
        filename,
        'edges',
        source={
            'label': 'Person',
            'csv_attribute': 'uid',
            'neo_attribute': 'uid'
        },
        target={
            'label': 'Company',
            'csv_attribute': 'company_number',
            'neo_attribute': 'uid'
        },
        directional=True,
        attributes=['appointment_type_label', 'appointment_date_formatted'],
        relationship_label='OFFICER_OF')
    write_csv_s3_neo(active_officers_humans_edges, filename, fs)


def create_company_officers_edges(active_officers_companies):
    active_officers_companies_edges = active_officers_companies[[
        'company_number', 'uid', 'appointment_date_formatted',
        'appointment_type_label'
    ]].copy()
    filename = 'active_officers_companies_edges'
    active_officers_companies_edges.drop_duplicates(inplace=True)
    create_file_record(
        filename,
        'edges',
        source={
            'label': 'Company',
            'csv_attribute': 'uid',
            'neo_attribute': 'uid'
        },
        target={
            'label': 'Company',
            'csv_attribute': 'company_number',
            'neo_attribute': 'uid'
        },
        directional=True,
        attributes=['appointment_type_label', 'appointment_date_formatted'],
        relationship_label='OFFICER_OF')
    write_csv_s3_neo(active_officers_companies_edges, filename, fs)


def create_probable_same_person_edges(persons_nodes):
    df_1 = persons_nodes[['uid', 'join_id']].replace(
        '', np.nan, regex=True).dropna().set_index('uid').copy()
    indexer = recordlinkage.index.Block('join_id')
    candidate_links = indexer.index(df_1)
    df_2 = pd.Series(index=candidate_links).reset_index()[['uid_1', 'uid_2']]
    df_2.columns = ['uid_x', 'uid_y']
    probable_id_edges = dedupe_edges_horizontally(df_2, 'uid_x', 'uid_y')
    filename = 'probable_id_edges'
    create_file_record(
        filename,
        'edges',
        source={
            'label': 'Person',
            'csv_attribute': 'uid_x',
            'neo_attribute': 'uid'
        },
        target={
            'label': 'Person',
            'csv_attribute': 'uid_y',
            'neo_attribute': 'uid'
        },
        directional=False,
        relationship_label='PROBABLY_SAME_PERSON')
    write_csv_s3_neo(probable_id_edges, filename, fs)


def write_csv_s3_neo(df, filename, fs):
    if df is None:
        print('Empty df, no CSV for {} written...'.format(filename))
    else:
        df = df.fillna('')
        with fs.open('{}{}.csv'.format(ROOT_DIR_OUTPUT, filename), 'w') as f:
            df.to_csv(f, chunksize=100000, index=False)
        print('Wrote {} to CSV'.format(filename))


def create_file_record(filename, file_type, **kwargs):
    record = {}
    record['public_url'] = S3_BASE + ROOT_DIR_OUTPUT + filename + '.csv'
    record['type'] = file_type
    if file_type == 'nodes':
        record['label'] = kwargs['label']
    elif file_type == 'edges':
        record['source'] = kwargs['source']
        record['target'] = kwargs['target']
        record['directional'] = kwargs['directional']
        record['relationship_label'] = kwargs['relationship_label']
        if 'attributes' in kwargs.keys():
            record['attributes'] = kwargs['attributes']
        else:
            record['attributes'] = None
    csv_file_records.append(record)


def create_person_uid(x, first_name_col, surname_col, month_year_birth_col,
                      post_code_col, backup_id_col):
    if not x.isnull().values.any():
        first_name = x[first_name_col].split(' ')[0]
        month_year = x[month_year_birth_col].strftime('%Y-%m')
        uid = first_name + '-' + x[surname_col] + '-' + month_year + '-' + x[
            post_code_col]
        uid = uid.upper()
        uid = uid.replace(' ', '')
        return uid
    else:
        uid = x[backup_id_col]
        uid = uid.upper()
        return uid


def create_target_company_uid(x):
    uk_identifiers = [
        'Companies House', 'England', 'Wales', 'Companies House',
        'United Kingdom', 'Scotland'
    ]
    if any(identifier in x['identification_place_registered'] for identifier in
           uk_identifiers) and x['identification_registration_number'] != '':
        output = x['identification_registration_number'].zfill(8).upper()
        return output
    else:
        output = x['etag'].upper()
        return output


def create_live_fake_id(x):
    if not x.isnull().values.any():
        normal_name = normalize_company_name(x['company_name'])
        output = normal_name.upper().strip(
        ) + '_' + x['regaddress_postcode'].upper().strip()
        return output
    else:
        output = np.nan
        return output


def perform_unique_check(df, index_column):
    if not df[index_column].is_unique:
        print('Warning id column is not unique! Some example...')
        print(df[df[index_column].duplicated(keep=False)].head(10))
    else:
        print('Index is unique')


def normalize_company_name(x):
    endings = [
        'LLP', 'LIMITED', 'LTD', 'L.T.D', 'PARTNERSHIP', 'LP', ' B.V.', 'PLC',
        'CO'
    ]
    output = x
    for end in endings:
        output = re.sub(r' {}$'.format(end), '', output)
    output = output.strip(' ')
    return output


def dedupe_edges_horizontally(df, source_col, target_col):
    if len(df) == 0:
        output = df
        print('Edges DataFrame is empty, nothing to dedupe')
    else:
        output = df[~pd.DataFrame(
            np.sort(df[[source_col, target_col]].values, axis=1)).duplicated()]
        output.reset_index(inplace=True)
    return output


def get_node_csvs(csv_file_records):
    node_records = [
        record for record in csv_file_records if record['type'] == 'nodes'
    ]
    return node_records


def get_edge_csvs(csv_file_records):
    edge_records = [
        record for record in csv_file_records if record['type'] == 'edges'
    ]
    return edge_records


def clear_graph(graph):
    count = 1
    while count > 0:
        count = graph.run(
            'MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(*) AS count'
        ).data()[0]['count']
    schema = Schema(graph)
    for label in schema.node_labels:
        constraints = schema.get_uniqueness_constraints(label)
        for constraint in constraints:
            schema.drop_uniqueness_constraint(label, constraint[0])
    print('All nodes and edges deleted, plus constraints dropped')


def create_constraints(constraint_labels, graph):
    for label in constraint_labels:
        graph.run(
            "CREATE CONSTRAINT ON (n:{}) ASSERT n.uid IS UNIQUE".format(label))
    print('Uniqueness constraints created...')


def create_all_nodes(node_csvs, graph):
    print('Running node creation queries...')
    for record in node_csvs:
        cypher = create_node_cypher(record['public_url'], record['label'])
        graph.run(cypher)


def create_all_edges(edge_csvs, graph):
    print('Running edge creation queries...')
    for record in edge_csvs:
        cypher = create_edges_cypher(
            record['public_url'], record['relationship_label'],
            record['source'], record['target'], record['attributes'],
            record['directional'])
        graph.run(cypher)


def create_node_cypher(location, label):
    string = ''
    temp_df = pd.read_csv(location, nrows=1)
    for col in temp_df.columns:
        string += col + ': ' + 'line.{}'.format(col) + ', '
    string = '{' + string[:-2] + '}'
    query = "USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM '{}' AS line CREATE (n:{} {})".format(
        location, label, string)
    print(query)
    return query


def create_edges_cypher(location, relationship_label, source, target,
                        attributes, directional):
    if attributes is None:
        attributes_query = ''
    else:
        attributes_query = ' SET '
        for attribute in attributes:
            attributes_query += 'r.{} = line.{}, '.format(attribute, attribute)
        attributes_query = attributes_query[:-2]
    query = "USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM '{location}' AS line MATCH (s:{source_label} {{ {source_neo}: line.{source_csv} }}), (t:{target_label} {{ {target_neo}: line.{target_csv} }}) CREATE (s)-[r:{relationship_label}]->(t){attributes_query}".format(
        location=location,
        source_label=source['label'],
        source_neo=source['neo_attribute'],
        source_csv=source['csv_attribute'],
        target_label=target['label'],
        target_neo=target['neo_attribute'],
        target_csv=target['csv_attribute'],
        relationship_label=relationship_label,
        attributes_query=attributes_query)
    print(query)
    return query


if __name__ == '__main__':
    main()
