# cse-545-project

Team members: Manal Shah(114362205), Dhruv Verma(113140590), Gauri Baraskar(114395197), Shreyashee Sinha(114709266)

Code Description: Perform similarity search using Minhashing and LSH based on crime type, demographics and crime rate 
at county level using FBI UCR dataset 1980-2020

Script Name and Line numbers where 2 data frameworks and 2 concepts are used: 
Big Data Pipeline Used: [1] Spark [2] Hadoop
Concepts Used: [1] Similarity Search [2] Hypothesis Testing

Pipelines:
[1] Spark : crime_similarity.py line 55, hypothesis_testing.py line 83
[2] Hadoop : Below are the locations and sizes of the datasets used

             Raw Dataset Size:
             cluster-d7bf-m:~$ hadoop fs -du -s dataset/Raw_files_FBI_dataset
             23.5555 [GB]    50585071790

             Processed Dataset Size
             cluster-d7bf-m:~$ hadoop fs -du -s dataset/Arrest_Data
             6.96124 [GB]    14949158438
             cluster-d7bf-m:~$ hadoop fs -du -s dataset/Arrest_Data_New
             3.91192 [GB]    8400775590

             Unemployment/Poverty/DUI Dataset Size 
             cluster-d7bf-m:~$ hadoop fs -du -s dataset/Other_data | awk '/^[0-9]+/ { print $1/(1024**3) " [GB]\t" $2 }'
             0.294721 [GB]   632909186

Concepts
[1] Similarity Search : crime_similarity.py
[2] Hypothesis Testing: hypothesis_testing.py

System Details: 
Master Node:
n1-standard-2, dataproc-2-0-ubu18-20220417-180200-rc01 , 64 GB Boot Disk Size 
Worker Nodes:
n1-highmem-4, dataproc-2-0-ubu18-20220417-180200-rc01, 48 GB Boot Disk Size

All ASR_[YEAR].csv files should be available at /dataset

To run similarity search for a specific year and month range:

spark-submit crime_similarity.py start_year end_year+1 start_month end_month+1

e.g. For years 2005-2010 and months 1-6: spark-submit crime_similarity.py 5 11 1 7
