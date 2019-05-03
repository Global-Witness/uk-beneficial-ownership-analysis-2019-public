#!/usr/bin/env

import sys
import pandas as pd
import s3fs
import re
import datetime
from everypolitician import EveryPolitician


root_dir = 'private-gw/psc-2019/'
fs = s3fs.S3FileSystem(
    key=sys.argv[1], secret=sys.argv[2],
    anon=False)  # create AWS S3 filesystem


def main():
    ep = EveryPolitician()
    legislatures = get_legislatures(ep.countries())
    politicians = get_people(legislatures)
    memberships = get_memberships(legislatures)
    organizations = get_organizations(legislatures)
    legislative_periods = get_legislative_periods(legislatures)
    politicians = create_additional_columns_every_politician(
        politicians, memberships, organizations, legislative_periods)
    filtered_politicians = filter_politicians_for_sufficient_date(politicians)
    write_csv_s3(filtered_politicians, 'politicians', fs)


def get_legislatures(countries):
    output = []
    for country in countries:
        legs = country.legislatures()
        output.extend(legs)
    return output


def get_legislative_periods(legislatures):
    output = []
    for leg in legislatures:
        leg_url = leg.popolo_url
        for period in leg.legislative_periods():
            leg_period_dict = {}
            leg_period_dict['leg_url'] = leg_url
            leg_period_dict['id'] = period.id
            leg_period_dict['start_date'] = period.start_date
            leg_period_dict['end_date'] = period.end_date
            leg_period_dict['country'] = period.country.name
            output.append(leg_period_dict)
    output_df = pd.DataFrame(output)
    output_df.set_index(['leg_url', 'id'], inplace=True)
    return output_df


def get_memberships(legislatures):
    output = []
    for leg in legislatures:
        leg_url = leg.popolo_url
        leg_popolo = leg.popolo()
        for membership in leg_popolo.memberships:
            member_dict = {}
            member_dict['leg_url'] = leg_url
            member_dict[
                'legislative_period_id'] = membership.legislative_period_id
            member_dict['person_id'] = membership.person_id
            member_dict['org_id'] = membership.organization_id
            output.append(member_dict)
    output_df = pd.DataFrame(output)
    return output_df


def get_organizations(legislatures):
    output = []
    for leg in legislatures:
        leg_popolo = leg.popolo()
        for org in leg_popolo.organizations:
            org_dict = {}
            org_dict['org_id'] = org.id
            org_dict['name'] = org.name
            output.append(org_dict)
    output_s = pd.DataFrame(output).set_index('org_id')['name']
    return output_s


def get_people(legislatures):
    output = []
    for leg in legislatures:
        leg_popolo = leg.popolo()
        leg_name = leg.name
        leg_country = leg.country.name
        for person in leg_popolo.persons:
            person_dict = {}
            person_dict['leg_name'] = leg_name
            person_dict['id'] = person.id
            person_dict['leg_country'] = leg_country
            person_dict['name'] = person.name
            person_dict['birth_date'] = person.birth_date
            output.append(person_dict)
    output_df = pd.DataFrame(output)
    output_df.set_index('id', drop=False, inplace=True)
    return output_df


def get_all_memberships(x, memberships, organizations):
    filtered_memberships = memberships[memberships.person_id == x]
    filtered_organizations = organizations[organizations.index.isin(
        filtered_memberships.org_id)]
    organizations_string = ', '.join(filtered_organizations.tolist())
    return organizations_string


def legislative_periods_active(x, memberships, legislative_periods):
    filtered_memberships = memberships[memberships.person_id == x][[
        'leg_url', 'legislative_period_id'
    ]]
    membership_tuples = [tuple(x) for x in filtered_memberships.values]
    filtered_legislative_periods = legislative_periods[
        legislative_periods.index.isin(membership_tuples)]
    active_periods_string = ', '.join(
        (filtered_legislative_periods.start_date.fillna('?') + ' -> ' +
         filtered_legislative_periods.end_date.fillna('?')).tolist())
    return active_periods_string


def create_additional_columns_every_politician(df, memberships, organizations,
                                               legislative_periods):
    temp_df = df.copy()
    temp_df['org_affiliation'] = temp_df.id.apply(
        get_all_memberships, args=(memberships, organizations))
    temp_df['active_periods'] = temp_df.id.apply(
        legislative_periods_active, args=(memberships, legislative_periods))
    temp_df['birth_date_type'] = temp_df.birth_date.fillna('').astype(
        str).apply(date_check)
    temp_df['first_name'] = temp_df.name.apply(lambda x: x.split(' ')[0] if
                                               len(x.split(' ')) > 1 else '')
    temp_df['last_name'] = temp_df.name.apply(lambda x: x.split(' ')[-1]
                                              if len(x.split(' ')) > 1 else '')
    temp_df['datetime_extracted'] = datetime.datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S")
    print('Created additional EveryPolitician columns....')
    return temp_df


def filter_politicians_for_sufficient_date(df):
    temp_df = df[df.birth_date_type.isin(['full date',
                                          'year and month'])].copy()
    temp_df['month_year'] = temp_df.birth_date.apply(lambda x: pd.to_datetime(
        x.earliest_date, errors='coerce'))
    temp_df['month_year'] = temp_df['month_year'].dt.strftime('%Y-%m')
    temp_df['join_id'] = temp_df.first_name.str.upper(
    ) + '-' + temp_df.last_name.str.upper() + '_' + temp_df['month_year']
    temp_df['join_id'] = temp_df['join_id'].str.replace(r'(NaT$)', '')
    temp_df['join_id'] = temp_df['join_id'].str.strip('-_ ')
    print('Added additional politician columns...')
    return temp_df


def date_check(date):
    if re.match((r'^\d{4}-\d{2}-\d{2}$'), date):
        return 'full date'
    if re.match((r'^\d{4}-\d{2}$'), date):
        return 'year and month'
    if re.match((r'^\d{4}$'), date):
        return 'year only'
    return ('unknown format')


def write_csv_s3(df, filename, fs):
    bytes_to_write = df.to_csv(None).encode()
    filename = filename
    with fs.open('{}processed/{}.csv'.format(root_dir, filename), 'wb') as f:
        f.write(bytes_to_write)
    print('Wrote {} to CSV'.format(filename))


if __name__ == '__main__':
    main()
