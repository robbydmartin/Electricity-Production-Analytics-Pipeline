import random
import json
from datetime import datetime, timezone

# json_schema = StructType() \
#     .add("period", StringType()) \
#     .add("respondent", StringType()) \
#     .add("respondent_name", StringType()) \
#     .add("fueltype", StringType()) \
#     .add("type-name", StringType()) \
#     .add("value", IntegerType()) \
#     .add("value-units", StringType())

CONSTANT_RESPONDENTS = ['AEC', 'AECI', 'AVA', 'AVRN', 'AZPS', 'BANC', 'BPAT', 'CAL', 
                        'CAR', 'CENT', 'CHPD', 'CISO', 'CPLE', 'CPLW', 'DEAA', 'DOPD', 
                        'DUK', 'EEI', 'EPE', 'ERCO', 'FLA', 'FMPP', 'FPC', 'FPL', 'GCPD', 
                        'GLHB', 'GRID', 'GRIF', 'GVL', 'GWA',' HGMA', 'HST', 'IID', 'IPCO', 
                        'ISNE', 'JEA', 'LDWP', 'LGEE', 'MIDA', 'MIDW', 'MISO', 'NE', 'NEVP', 
                        'NSB', 'NW', 'NWMT', 'NY', 'NYIS', 'PACE', 'PACW', 'PGE', 'PJM', 
                        'PNM', 'PSCO', 'PSEI', 'SC', 'SCEG', 'SCL', 'SE', 'SEC', 'SEPA', 
                        'SIKE', 'SOCO', 'SPA', 'SRP', 'SW', 'SWPP', 'TAL', 'TEC', 'TEN', 
                        'TEPC', 'TEX', 'TIDC', 'TPWR', 'TVA', 'US48', 'WACM', 'WALC', 'WAUW', 'WWA']

CONSTANT_RESPONDENTS_NAMES = [  'PowerSouth Energy Cooperative', 'Associated Electric Cooperative, Inc.', 'Avista Corporation',
                                'Avangrid Renewables, LLC', 'Arizona Public Service Company', 'Balancing Authority of Northern California',
                                'Bonneville Power Administration', 'California', 'Carolinas', 'Central', 'Public Utility District No. 1 of Chelan County',
                                'California Independent System Operator', 'Duke Energy Progress East', 'Duke Energy Progress West', 'Arlington Valley, LLC',
                                'PUD No. 1 of Douglas County', 'Duke Energy Carolinas', 'Electric Energy, Inc.', 'El Paso Electric Company',
                                'Electric Reliability Council of Texas, Inc.', 'Florida', 'Florida Municipal Power Pool', 'Duke Energy Florida, Inc.', 
                                'Florida Power & Light Co.', 'Public Utility District No. 2 of Grant County, Washington', 'GridLiance', 
                                'Gridforce Energy Management, LLC', 'Griffith Energy, LLC', 'Gainesville Regional Utilities', 'NaturEner Power Watch, LLC', 
                                'New Harquahala Generating Company, LLC', 'City of Homestead', 'Imperial Irrigation District', 'Idaho Power Company', 
                                'ISO New England', 'JEA', 'Los Angeles Department of Water and Power', 
                                'LG&E and KU Services Company as agent for Louisville Gas and Electric Company and Kentucky Utilities Company', 
                                'Mid-Atlantic', 'Midwest', 'Midcontinent Independent System Operator, Inc.', 'New England', 'Nevada Power Company', 
                                'Utilities Commission of New Smyrna Beach', 'Northwest', 'NorthWestern Corporation', 'New York', 'New York Independent System Operator', 
                                'PacifiCorp East', 'PacifiCorp West', 'Portland General Electric Company', 'PJM Interconnection, LLC', 'Public Service Company of New Mexico', 
                                'Public Service Company of Colorado', 'Puget Sound Energy, Inc.', 'South Carolina Public Service Authority', 
                                'Dominion Energy South Carolina, Inc.', 'Seattle City Light', 'Southeast', 'Seminole Electric Cooperative', 
                                'Southeastern Power Administration', 'Sikeston Board of Municipal Utilities', 'Southern Company Services, Inc. - Trans', 
                                'Southwestern Power Administration', 'Salt River Project Agricultural Improvement and Power District', 'Southwest', 
                                'Southwest Power Pool', 'City of Tallahassee', 'Tampa Electric Company', 'Tennessee', 'Tucson Electric Power', 'Texas', 
                                'Turlock Irrigation District', 'City of Tacoma, Department of Public Utilities, Light Division', 'Tennessee Valley Authority', 
                                'United States Lower 48', 'Western Area Power Administration - Rocky Mountain Region', 
                                'Western Area Power Administration - Desert Southwest Region', 'Western Area Power Administration - Upper Great Plains West', 'NaturEner Wind Watch, LLC']

CONSTANT_FUEL_TYPES = ['BAT', 'BAT', 'COL', 'GEO', 'NG', 'NUC', 'OES', 'OIL', 'OTH', 'PS', 'SNB', 'SNB', 'SUN', 'UES', 'UES', 'UNK', 'WAT', 'WNB', 'WND']
CONSTANT_FUEL_MULTIPLIER = [0.1, 0.1, 0.7, 0.01, 0.9, 0.8, 0.01, 1.0, 0.01, 0.01, 0.2, 0.2, 0.6, 0.01, 0.01, 0.01, 0.1, 0.2, 0.2]
CONSTANT_FUEL_TYPE_NAMES = ['Battery', 'Battery storage', 'Coal', 'Geothermal', 'Natural Gas', 'Nuclear', 'Other energy storage', 'Petroleum', 'Other', 'Pumped storage', 
                            'Solar with integrated battery storage', 'Solar Battery', 'Solar', 'Unknown energy storage', 'Unknown Energy', 'Unknown', 'Hydro', 
                            'Wind with integrated battery storage', 'Wind']


def create_single_record() -> dict:

    respondent = random.choice(CONSTANT_RESPONDENTS)
    respondent_index = CONSTANT_RESPONDENTS.index(respondent)
    fueltype = random.choice(CONSTANT_FUEL_TYPES)
    fueltype_index = CONSTANT_FUEL_TYPES.index(fueltype)

    record = {
        "period" : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H"),
        "respondent" : respondent,
        "respondent_name" : CONSTANT_RESPONDENTS_NAMES[respondent_index],
        "fueltype" : fueltype,
        "type-name" : CONSTANT_FUEL_TYPE_NAMES[fueltype_index], 
        "value" : int(random.randint(1, 999) * CONSTANT_FUEL_MULTIPLIER[fueltype_index]),
        "value-units" : "megawatthours"
    }

    return record

def create_multiple_records(num_records: int) -> list:
    records = [create_single_record() for _ in range(num_records)]
    return records
