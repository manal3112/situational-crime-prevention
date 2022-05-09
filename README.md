# cse-545-project
All ASR_[YEAR].csv files should be available at /dataset

To run similarity search for a specific year and month range:

spark-submit crime_similarity.py start_year end_year+1 start_month end_month+1

e.g. For years 2005-2010 and months 1-6: spark-submit crime_similarity.py 5 11 1 7
