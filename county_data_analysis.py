###############################################################################################################################
## Team members: Manal Shah(114362205), Dhruv Verma(113140590), Gauri Baraskar(114395197), Shreyashee Sinha(114709266)
##
## Code Description: Creates dataframes for counties in consideration for each crime type in a given
## year range and then saves them into CSV files for each county. These CSVs are to be used for hypothesis testing.
##
## Script Name and Line numbers where 2 data frameworks and 2 concepts are used: 
## Big Data Pipeline Used: [1] Spark [2] Hadoop
## Concepts Used: [1] Similarity Search [2] Hypothesis Testing
##
## Pipelines:
## [1] Spark : crime_similarity.py line 75, hypothesis_testing.py line 120
## [2] Hadoop : Below are the locations and sizes of the datasets used 
##              Raw Dataset Size:
##              cluster-d7bf-m:~$ hadoop fs -du -s dataset/Raw_files_FBI_dataset | awk '/^[0-9]+/ { print $1/(1024**3) " [GB]\t" $2 }'
##              23.5555 [GB]    50585071790

##              Processed Dataset Size
##              cluster-d7bf-m:~$ hadoop fs -du -s dataset/Arrest_Data | awk '/^[0-9]+/ { print $1/(1024**3) " [GB]\t" $2 }'
##              6.96124 [GB]    14949158438
##              cluster-d7bf-m:~$ hadoop fs -du -s dataset/Arrest_Data_New | awk '/^[0-9]+/ { print $1/(1024**3) " [GB]\t" $2 }'
##              3.91192 [GB]    8400775590

##              Unemployment/Poverty/DUI Dataset Size 
##              cluster-d7bf-m:~$ hadoop fs -du -s dataset/Other_data | awk '/^[0-9]+/ { print $1/(1024**3) " [GB]\t" $2 }'
##              0.294721 [GB]   632909186
##
## Concepts
## [1] Similarity Search : crime_similarity.py
## [2] Hypothesis Testing: hypothesis_testing.py
##
## System Details: 
## Master Node:
## n1-standard-2, dataproc-2-0-ubu18-20220417-180200-rc01 , 64 GB Boot Disk Size 
## Worker Nodes:
## n1-highmem-4, dataproc-2-0-ubu18-20220417-180200-rc01, 48 GB Boot Disk Size

import pandas as pd
import re

# Map to build FIPS code
state_to_FIPS = {
    'AL':'01',
    'AK':'02',
    'AZ':'04',
    'AR':'05',
    'CA':'06',
    'CO':'08',
    'CT':'09',
    'DE':'10',
    'FL':'12',
    'GA':'13',
    'HI':'15',
    'ID':'16',
    'IL':'17',
    'IN':'18',
    'IA':'19',
    'KS':'20',
    'KY':'21',
    'LA':'22',
    'ME':'23',
    'MD':'24',
    'MA':'25',
    'MI':'26',
    'MN':'27',
    'MS':'28',
    'MO':'29',
    'MT':'30',
    'NE':'31',
    'NV':'32',
    'NH':'33',
    'NJ':'34',
    'NM':'35',
    'NY':'36',
    'NC':'37',
    'ND':'38',
    'OH':'39',
    'OK':'40',
    'OR':'41',
    'PA':'42',
    'RI':'44',
    'SC':'45',
    'SD':'46',
    'TN':'47',
    'TX':'48',
    'UT':'49',
    'VT':'50',
    'VA':'51',
    'WA':'52',
    'WV':'54',
    'WI':'55',
    'WY':'56',
    'AS':'60',
    'GU':'66',
    'MP':'69',
    'PR':'72',
    'VI':'78',
}

def convert_ORI_to_FIPS(ORI_code):
    county_code = ORI_code[2:5]
    state = ORI_code[0:2]
    state_number = state_to_FIPS[state]
    FIPS_Code = state_number + county_code
    return FIPS_Code

fips_mapping = {
    '48HPD': '48201',
    '48SPD': '48029',
    '17CPD': '17031',
    '42PEP': '42101',
    '13APD': '13121'
}

# Read the similarity output files
with open('SIM_OUTPUT_2016-2020.txt') as f:
    dictionary = {}
    rx = re.compile("-----------------------------FBI AGENCY:")
    while True:
        line = f.readline()
        if line:
            m = rx.search(line)
            if m:
                next_line = f.readline()
                parts = next_line.split(",")
                array = parts[1].split(" ")
                target_agency = array[3].split(":")[1]
                target_county = convert_ORI_to_FIPS(target_agency)
                if target_county in fips_mapping:
                    target_county = fips_mapping[target_county]
                if any(c.isalpha() for c in target_county):
                    continue
                #target_agency = final_part
                ## Find similar matches
                l = f.readline()
                dictionary[target_county] = []
                for i in range(20):
                    try:
                        l = f.readline()
                        similar_agency = l.split(",")[0]
                        if len(similar_agency) > 3:
                            similar_agency = similar_agency[-8:-1]
                            similar_county = convert_ORI_to_FIPS(similar_agency)
                            #print(similar_county)
                            if similar_county in fips_mapping:
                                similar_county = fips_mapping[similar_county]
                            if any(c.isalpha() for c in similar_county):
                                continue
                            dictionary[target_county].append(similar_county)
                    except:
                        print("Parsing error")
        else:
            break

import os

combined_df = None
for i in range(16, 21):
    if i >= 10:
        name = "ASR_20" + str(i) + ".csv"
    else:
        name = "ASR_200" + str(i) + ".csv"
    full_path = os.path.join("Arrest_Data_new", name)
    print("Reading:", full_path)
    data = pd.read_csv(full_path)
    combined_df = pd.concat([combined_df, data])

combined_df['County_code'] = combined_df.ORI_Code.str[2:5]
combined_df['State'] = combined_df.ORI_Code.str[0:2]
combined_df['State_number'] = combined_df['State'].map(state_to_FIPS)
combined_df['FIPS_Code'] = combined_df['State_number'] + combined_df['County_code']
combined_df['FIPS_Code'] = combined_df['FIPS_Code'].replace(fips_mapping)
combined_df.drop(['County_code','State','State_number'],axis=1, inplace=True)

combined_df.drop(['ORI_Code','Month','County','Suburban','MSA','State_Code','Division','Group','Agency_Name','State_Name'],axis=1,inplace=True)
#combined_df.groupby([]).mean()

population_df = combined_df.groupby(['FIPS_Code','Year']).agg({'Population_for_Agency':'mean'}).reset_index()

combined_df.drop(['Population_for_Agency'],axis=1,inplace=True)

combined_df = combined_df.groupby(['FIPS_Code','Year','Offense_Code']).sum()

combined_df['Arrests']= combined_df.iloc[:, 12:].sum(axis=1)

combined_df = combined_df[combined_df.columns.drop(list(combined_df.filter(regex='Female-')))]
combined_df = combined_df[combined_df.columns.drop(list(combined_df.filter(regex='Male-')))]
combined_df = combined_df[combined_df.columns.drop(list(combined_df.filter(regex='Adult-')))]

combined_df = combined_df[combined_df.columns.drop(list(combined_df.filter(regex='Juvenile-')))]

combined_df = combined_df.reset_index()

df = pd.merge(combined_df, population_df, on=["FIPS_Code", "Year"], how="left")

df['Arrest_rate'] = df["Arrests"].div(df["Population_for_Agency"].values)

df.drop(['Arrests','Population_for_Agency'],axis=1,inplace=True)

df = df.set_index(['FIPS_Code','Year','Offense_Code'])['Arrest_rate'].unstack().reset_index()

df.fillna(0, inplace=True)

for target_county, similar_counties in dictionary.items():
    file_name = target_county + "_2016-2020_data.csv"
    filtered_data = df[df['FIPS_Code'].isin(similar_counties)]
    print("Saving:", file_name)
    filtered_data.to_csv(file_name, index=False)