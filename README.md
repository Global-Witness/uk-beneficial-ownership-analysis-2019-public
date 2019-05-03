# Persons of Significant Control Analysis March 2019

This is an updated analysis of the UK Persons of Significant control (beneficial ownership) data including scripts on how to import UK Companies House Data into Neo4J. The original analysis undertaken in 2018, the accompanying briefing *Getting the UK's House in Order (2019)*. The original 2018 report *The Companies We Keep* is available  [here](https://www.globalwitness.org/en/campaigns/corruption-and-money-laundering/anonymous-company-owners/companies-we-keep/) and the code of the original analysis [here](https://github.com/Global-Witness/the-companies-we-keep-public).

## Analysis

The analysis is documented and available as a Jupyter Notebook at [`notebooks/psc_analysis_2019.ipynb`](notebooks/psc_analysis_2019.ipynb).

## Data sources

- UK Companies House
	- [PSC snapshot](http://download.companieshouse.gov.uk/en_pscdata.html)
	- [Free company data product](http://download.companieshouse.gov.uk/en_output.html)
	- Officer appointment bulk data (available on request)
- [EveryPolitician](http://everypolitician.org/)
	- We used the dedicated [EveryPolitician Python library](https://github.com/everypolitician/everypolitician-python) to download relevant data on politicians. Data is also available to search and download [here](http://everypolitician.org/countries.html)
- [Tax Justice Network Financial Secrecy Index](https://www.financialsecrecyindex.com/introduction/fsi-2018-results)

## Data dictionaries

The data dictionaries here are for the key processed data files. Only the fields that are used in the analysis are defined here.

`live_companies.csv` - clean version of [Companies House free company data product](http://download.companieshouse.gov.uk/en_output.html) with some additional fields. Contains all companies and partnerships Companies House considers to be part of the "live register" (as of 1st March 2019).

  - `company_number`: String, unique company number assigned by Companies House
  - `incorporation_date_formatted`: Date, date of incorporation of company
  - `company_type`: String, company type e.g. Limited Liability Partnership deduced from first two characters of company number using [List of Companies and Prefixes](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/426891/uniformResourceIdentifiersCustomerGuide.pdf)
  - `first_and_postcode`: String, combination of first line of address and postcode
  - `psc_regime_applies`: Boolean, marks whether or not the PSC regime applies to that company

`active_psc_records.csv` - active (as of 5th March 2019) PSCs, 1 row per PSC. Distinct from statements (see below) where no information on PSCs is given e.g. the company states it has no PSC:

  - `company_number`: String, unique company number (of the company filing PSC information) assigned by Companies House
  - `address_country_normal`: String, cleaned version of address country field
  - `country_of_residence_normal`: String, cleaned version of country of residence field
  - `natures_of_control`: List, the methods through which the PSC exercises control e.g. via share ownership
  - `kind`: String, type of PSC e.g. person, company or legal person
  - `address_country`: String, address country
  - `join_id`: String, combination of first name, last name and month and year of birth. Useful for grouping individuals
  - `non_rle_country`: Boolean, True if PSC is a company and is registered or has address in a country without a recognised stock exchange
  - `secret_base`: Boolean, True if PSC country of residence, address country or registered country is a country that scores 60 or above in [Tax Justice Network Financial Secrecy Index 2018](https://www.financialsecrecyindex.com/introduction/fsi-2018-results)
  
`active_psc_statements.csv` - active (as of 5th March 2019) statements where filing company is not submitting information on a PSC e.g. they have not completed steps to find PSC. 1 row per statement:

  - `company_number`: String, unique company number (of the company filing PSC information) assigned by Companies House
  - `statement`: String, text containing the statement type e.g. 'steps-to-find-psc-not-yet-completed'

`active_officers.csv` - active (as of 1st March 2019) company officers (e.g. secretaries and directors)

  - `company_number`: String, unique company number (of the company filing PSC information) assigned by Companies House
  - `address_country_normal`: String, cleaned version of address country field
  - `country_of_residence_normal`: String, cleaned version of country of residence field
  - `appointment_type`: String, type of appointment e.g. 'Current Director'
  - `company_number`: String, unique company number (of the company filing officer information) assigned by Companies House
  - `possible_politician`: Field definition
  - `secret_base`: Boolean, True if officer country of residence, address country or registered country is a country that scores 60 or above in [Tax Justice Network Financial Secrecy Index 2018](https://www.financialsecrecyindex.com/introduction/fsi-2018-results)
  - `join_id`: String, combination of first name, last name and month and year of birth. Useful for grouping individuals
  
  `active_psc_exemptions.csv` - active (as of 5th March 2019) trading exemptions for filing companies:
  `active_psc_controls.csv` - transformation of `active_psc_records.csv` except there is one row per nature/method of control for each PSC record

## Data processing steps

The cleaning and processing steps are set out in [`scripts/process_company_data.py`](scripts/process_company_data.py). In brief they are:

- Download EveryPolitician data
- Load and clean live companies file from
- Load, transform (from JSON to CSV) and clean PSC snapshot
- Load and clean disqualified directors data
- Load and clean officers data
- Add additional fields for analysis (see [Data Dictionaries](#data-dictionaries))
- Export processed, analysis-ready CSV files
- Create intermediary CSV files (nodes and edges) using clean data to then load into Neo4J graph

Additional processing of the raw .dat files for appointments and disqualified directors data is set out in [this repo](https://github.com/Global-Witness/uk-companies-house-parsers-public).

The scripts in this repository should be run in the following order:

```
python everypolitician_retrieve.py
python process_company_data.py
python neo4j_transform_load.py
```

## Neo4J graph

[`process_company_data.py`](scripts/process_company_data.py) creates a Neo4J graph of the company data. The basic model contains the following nodes types:

- `Person` - officer or Person of Significant Control
- `Company` - a company from the live company dataset, corporate officer or corporate person of significant control
- `Statement` - a statement made by a company explaining why they are not filing a PSC e.g. they have not yet completed the steps
- `LegalPerson` - a PSC that is a Legal Person e.g. The Treasury
- `Exemption` - a PSC exemption filed by a company
- `SuperSecure` - a SuperSecure PSC with protected details
- `Postcode` - a postcode where companies from the live company dataset are registered

An example (using data from 1st March 2019) of the part of the graph visualised using [Linkurious](https://linkurio.us/):

![Example of company structure visualised](images/linkurious.png?raw=true "Linkurious example")

## Requirements

Python library requirements are given in [`requirements.txt`](requirements.txt). Amazon Web Services EC2 x1e.xlarge instance was used for processing and analysis due to the size of the data.

## Get in touch

If you have any questions regarding this analysis, please get in touch with Sam Leon on sleon@globalwitness.org.

## License

This work is published under the [Creative Commons ShareAlike 4.0 International License](https://creativecommons.org/licenses/by-sa/4.0/legalcode).
