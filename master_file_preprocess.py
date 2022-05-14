# -*- coding: utf-8 -*-
###############################################################################################################################
## Team members: Manal Shah(114362205), Dhruv Verma(113140590), Gauri Baraskar(114395197), Shreyashee Sinha(114709266)
##
## Code Description: Master file parsing to extract data using helpfile: 
# https://s3-us-gov-west-1.amazonaws.com/cg-d4b776d0-d898-4153-90c8-8336f86bdfec/masters/asr/asr-help.zip using FBI UCR dataset 1980-2020
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
## Ran on Google Colab
## master_file_preprocess.ipynb exported as master_file_preprocess.py

from google.colab import drive
drive.mount('/content/gdrive/', force_remount = True)

import os
import pandas as pd
# os.chdir('/content/gdrive/MyDrive/CSE_545_Project/Raw_files_FBI_dataset/')
os.chdir('/content/gdrive/MyDrive/BDA/CSE_545_Project/Raw_files_FBI_dataset/1985-2000_dat_files/')

directory = "/content/gdrive/MyDrive/BDA/CSE_545_Project/Raw_files_FBI_dataset/1985-2000_dat_files"

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if os.path.isfile(f):
        print(f, f.split('/')[-1].split('.')[0])

directory = '/content/gdrive/MyDrive/CSE_545_Project/Raw_files_FBI_dataset/'
# directory = '/content/gdrive/MyDrive/BDA/CSE_545_Project/Raw_files_FBI_dataset/1985-2000_dat_files'
for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if os.path.isfile(f) and f.lower().endswith('.dat'):
    # if os.path.isfile(f) and f.lower().endswith('.txt') and f.split('/')[-1].split('.')[0] in ['ASR_2017']:
      try:
        print("Currently processing:", f)
        df = pd.read_csv(f, names = ["Column_1", "Column_2"])
        df.drop(columns = ["Column_2"], inplace = True)
        # df["Len"] = df["Column_1"].apply(lambda x: len(x))
        # df.drop(labels = df[df["Len"] != 471].index, inplace = True)

        # Extract State Code, ORI Code, Group, Division, Year, Month, Offense Code from each line 
        df["State_Code"] = df["Column_1"].apply(lambda x: int(x[1:3]))
        df["ORI_Code"]= df["Column_1"].apply(lambda x: x[3:10])
        df["Group"] = df["Column_1"].apply(lambda x: x[10:12])
        df["Division"] = df["Column_1"].apply(lambda x: (x[12]))
        df["Year"] = df["Column_1"].apply(lambda x: int(x[15:17]))
        df["Month"] = df["Column_1"].apply(lambda x: x[13:15])
        df["Offense_Code"] = df["Column_1"].apply(lambda x: x[17:20])

        # Identify Agency and Monthly headers, they have different attributes 
        df["Agency_Header"] = df["Month"] == "00"
        df["Monthly_Header"] = df["Offense_Code"] == "000"

        # Extract MSA, County, Suburban, Population For Agency, Agency Name and State Name from Agency Headers
        df.loc[ df["Agency_Header"], "MSA"] = df.loc[ df["Agency_Header"], "Column_1"].apply(lambda x: x[17:20])
        df.loc[ df["Agency_Header"], "County"] = df.loc[ df["Agency_Header"], "Column_1"].apply(lambda x: x[20:23])
        
        df.loc[ df["Agency_Header"], "Suburban"] = df.loc[ df["Agency_Header"], "Column_1"].apply(lambda x: int(x[28]))
        df.loc[ df["Agency_Header"], "Population_for_Agency"] = df.loc[ df["Agency_Header"], "Column_1"].apply(lambda x: int(x[31:40]))
        df.loc[ df["Agency_Header"], "Agency_Name"] = df.loc[ df["Agency_Header"], "Column_1"].apply(lambda x: x[40:64])
        df.loc[ df["Agency_Header"], "State_Name"] = df.loc[ df["Agency_Header"], "Column_1"].apply(lambda x: x[64:70])

        df["Agency_Name"] = df["Agency_Name"].str.strip()

        # Update extracted info for all detail rows
        def updateData(x):
            global msa, county, suburban, population, state_name, name
            if x["Agency_Header"]:
                name = x["Agency_Name"]
                state_name = x["State_Name"]
                population = x["Population_for_Agency"]
                suburban = x["Suburban"]
                county = x["County"]
                msa = x["MSA"]
                
            x["Agency_Name"] = name
            x["State_Name"] = state_name
            x["Population_for_Agency"] = population
            x["Suburban"] = suburban
            x["County"] = county
            x["MSA"] = msa
            return x
            
        msa, county, suburban, population, state_name, name = None, None, None, None, None, None
        df = df.apply(lambda x: updateData(x), axis = 1)

        arrest_columns = ['Adult-White', 'Adult-Black', 'Adult-Indian',
                          'Adult-Asian', 'Juvenile-White', 'Juvenile-Black', 'Juvenile-Indian',
                          'Juvenile-Asian', 'Adult-Hispanic', 'Adult-Non-Hispanic',
                          'Juvenile-Hispanic', 'Juvenile-Non-Hispanic', 'Male-Under-10',
                          'Male-10-12', 'Male-13-14', 'Male-15', 'Male-16', 'Male-17', 'Male-18',
                          'Male-19', 'Male-20', 'Male-21', 'Male-22', 'Male-23', 'Male-24',
                          'Male-25-29', 'Male-30-34', 'Male-35-39', 'Male-40-44', 'Male-45-49',
                          'Male-50-54', 'Male-55-59', 'Male-60-64', 'Male-Over-64',
                          'Female-Under-10', 'Female-10-12', 'Female-13-14', 'Female-15',
                          'Female-16', 'Female-17', 'Female-18', 'Female-19', 'Female-20',
                          'Female-21', 'Female-22', 'Female-23', 'Female-24', 'Female-25-29',
                          'Female-30-34', 'Female-35-39', 'Female-40-44', 'Female-45-49',
                          'Female-50-54', 'Female-55-59', 'Female-60-64', 'Female-Over-64']

        df[arrest_columns] = None
        code_column_map = dict(zip(range(1,57),arrest_columns))

        #Populate Arrests for each of the rows under that Agency
        def populateArrests(x):
            if not x["Agency_Header"] and not x["Monthly_Header"]:
                global bad_entries
                try:
                    for i in range(23,471,8):
                        code_arrest_group = x["Column_1"][i:i+8]
                        code = int(code_arrest_group[:3])
                        arrests = int(code_arrest_group[3:])
                        if code_column_map.get(code):
                            x[code_column_map[code]] = arrests 
                except Exception as ex:
                    bad_entries += 1
            return x
            
        bad_entries = 0
        df = df.apply(populateArrests, axis = 1)
        print("Total Bad Entries Found:{}".format(bad_entries))

        final_cols = ['State_Code', 'Year', 'Division', 'ORI_Code',
                      'Group', 'Month', 'Offense_Code',
                      'MSA', 'County', 'Suburban', 'Agency_Name', 'State_Name',
                      'Population_for_Agency', 'Adult-White', 'Adult-Black', 'Adult-Indian',
                      'Adult-Asian', 'Juvenile-White', 'Juvenile-Black', 'Juvenile-Indian',
                      'Juvenile-Asian', 'Adult-Hispanic', 'Adult-Non-Hispanic',
                      'Juvenile-Hispanic', 'Juvenile-Non-Hispanic', 'Male-Under-10',
                      'Male-10-12', 'Male-13-14', 'Male-15', 'Male-16', 'Male-17', 'Male-18',
                      'Male-19', 'Male-20', 'Male-21', 'Male-22', 'Male-23', 'Male-24',
                      'Male-25-29', 'Male-30-34', 'Male-35-39', 'Male-40-44', 'Male-45-49',
                      'Male-50-54', 'Male-55-59', 'Male-60-64', 'Male-Over-64',
                      'Female-Under-10', 'Female-10-12', 'Female-13-14', 'Female-15',
                      'Female-16', 'Female-17', 'Female-18', 'Female-19', 'Female-20',
                      'Female-21', 'Female-22', 'Female-23', 'Female-24', 'Female-25-29',
                      'Female-30-34', 'Female-35-39', 'Female-40-44', 'Female-45-49',
                      'Female-50-54', 'Female-55-59', 'Female-60-64', 'Female-Over-64']

        # Exclude Agency and MOnthly headers from final processed df
        final_df = df[(~df["Agency_Header"]) & (~df["Monthly_Header"])][final_cols]

        fname = "{}_new.csv".format(f.split('/')[-1].split('.')[0])
        final_df.to_csv(fname, index = False)
        print("{} File Processed".format(f.split('/')[-1].split('.')[0]))
      except Exception as e:
        print("Error processing:", f)
        print(e)
        pass