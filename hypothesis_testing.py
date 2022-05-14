###############################################################################################################################
## Team members: Manal Shah(114362205), Dhruv Verma(113140590), Gauri Baraskar(114395197), Shreyashee Sinha(114709266)
##
## Code Description: Perform hypothesis testing for each county we considered and the given year range for them. Outputs 
## the the correlation coefficient, p-values and corrected p-values for crime types or crime in general.
##
## Script Name and Line numbers where 2 data frameworks and 2 concepts are used: 
## Big Data Pipeline Used: [1] Spark [2] Hadoop
## Concepts Used: [1] Similarity Search [2] Hypothesis Testing
##
## Pipelines:
## [1] Spark : crime_similarity.py line 55, hypothesis_testing.py line 120
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

import pyspark
import os
import csv
import argparse
import numpy as np
import math
from scipy.stats import t
import math

# Create county data tuples to join 
# with the feature we wish to test.
def create_county_tuples(tup):
  crime_data_header_dict = crime_dict.value
  fips_code = tup[crime_data_header_dict['FIPS_Code']]
  year = tup[crime_data_header_dict['Year']]
  if int(year) in range(10, 21):
    year = '20' + year
  elif int(year) in range(1, 10):
    year = '200' + year
  for key, value in crime_data_header_dict.items():
    if key not in ['FIPS_Code', 'Year']:
      yield ((int(fips_code), int(year)), (key, float(tup[value])))

# Create feature data with the FIPS_Code
# and Year as keys. This will be joined
# with the county data created in 'create_county_tuples().'
def create_feature_tuples(tup):
  feature_data_header_dict = feature_dict.value
  fips_code = tup[feature_data_header_dict['FIPS_Code']]
  years = years_list.value
  for year in years:
    col_name = feature_name.value + "_" + year
    rate = tup[feature_data_header_dict[col_name]].strip()
    if len(rate) > 0:
      yield ((int(fips_code), int(year)), float(rate))

# Compute the correlation values between two lists
def get_correlation_val(data):
  offence_code = data[0]
  arrest_rate = []
  feature_rate = []
  for arrest_val, unemploy_val in data[1]:
    arrest_rate.append(arrest_val)
    feature_rate.append(unemploy_val)
  corr = np.corrcoef(arrest_rate, feature_rate)
  return (offence_code, (corr[0][1], len(arrest_rate) - 2))

# Compute the p-values and corrected 
# p-values for correlated data.
def get_p_values(tup):
  offence_type = tup[0]
  correction_val = 43 # Number of offence codes
  pearson_corr, df = tup[1]
  t_stat = pearson_corr/math.sqrt((1 - (pearson_corr**2))/df)
  p_value = 2 * (1 - t.cdf(abs(t_stat), df))
  corrected_p_value = p_value * correction_val
  if math.isnan(pearson_corr):
    pearson_corr = 0
  if math.isnan(p_value):
    p_value = 0
  if math.isnan(corrected_p_value):
    corrected_p_value = 0
  return (offence_type, (pearson_corr, p_value, corrected_p_value))

# Args for running this code
def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument("crime_data_path", type=str) # Path containing all data frames for crime data
  parser.add_argument("feature_data_path", type=str) # Path to the dataframe we wish to test as a hypothesis 
  parser.add_argument("feature_name", type=str) # Feature name which we are testing
  parser.add_argument('--years_list', nargs='+', default=['2001', '2002', '2003', '2004', '2005'])
  return parser.parse_args()

if __name__ == "__main__":
  args = parse_args()
  print(args)

  crime_data_path = args.crime_data_path
  feature_data_path = args.feature_data_path
  feature_name = args.feature_name
  years_list = args.years_list

  sc = pyspark.SparkContext()

  for i, data_path in enumerate(os.listdir(crime_data_path)):
    full_path = os.path.join(crime_data_path, data_path)
    print("Processing:", full_path)
    crime_data = sc.textFile(full_path)
    feature_data = sc.textFile(feature_data_path)
    crime_data = crime_data.mapPartitions(lambda line: csv.reader(line))
    feature_data = feature_data.mapPartitions(lambda line: csv.reader(line))

    crime_data_header = crime_data.first()
    feature_data_header = feature_data.first()

    crime_data_header_dict = {k.strip(): v for v, k in enumerate(crime_data_header)}
    feature_data_header_dict = {k.strip(): v for v, k in enumerate(feature_data_header)}
    crime_data_header_dict.pop('', None)

    # Create broadcast vars 
    crime_dict = sc.broadcast(crime_data_header_dict)
    feature_dict = sc.broadcast(feature_data_header_dict)
    if i == 0:
      years_list = sc.broadcast(years_list)
      feature_name = sc.broadcast(feature_name)

    # Get the data in the needed format [((FIPS_Code, Year), ('Crime_type', val))]
    # and [((FIPS_Code, Year), rate)]
    crime_temp = crime_data.filter(lambda row: row != crime_data_header).flatMap(lambda t: create_county_tuples(t))
    feature_temp = feature_data.filter(lambda row: row != feature_data_header).flatMap(lambda t: create_feature_tuples(t))

    # Join the data and compute corr values, p-values and corrected p-values
    joined_data = feature_temp.join(crime_temp)
    grouped_joined_data = joined_data.groupByKey().mapValues(list)
    flattened_grouped_joined_data = grouped_joined_data.flatMap(lambda tup: tup[1])
    flattened_grouped_joined_data = flattened_grouped_joined_data.map(lambda tup: (tup[1][0], (tup[1][1], tup[0])))
    grouped_by_offence_type = flattened_grouped_joined_data.groupByKey().mapValues(list)
    corr_list = grouped_by_offence_type.map(lambda tup: get_correlation_val(tup)).map(lambda tup: get_p_values(tup)).collect()
    corr_list = sorted(corr_list, key=lambda tup: tup[1][0], reverse=True)
    file_name = data_path.split("_")[0] + "_" + feature_name.value + "_corr_" + years_list.value[0] + "-" + years_list.value[-1] + ".txt"
    with open(file_name, "w") as file1:
      for corr in corr_list:
        file1.write(str(corr))
        file1.write("\n")
