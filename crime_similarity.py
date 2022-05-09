##########################################################################
##
## Perform similarity search based on crime type, demographics and crime rate 
## at county level using FBI UCR dataset 1980-2020
##
## Author: Manal Shah
## SBU ID: 114362205

from data_preprocess import get_aggregate_data
from pyspark import SparkContext
from pprint import pformat
import sys
import time
import logging
import csv
import binascii
import random
import math

if __name__ == "__main__":

    # Logging Setup
    logging.basicConfig(filename="crime_similarity_OUTPUT_new.txt", 
					format='%(asctime)s %(message)s', 
					filemode='w')
    logger=logging.getLogger()
    logger.setLevel(logging.INFO)

    # Input ranges
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    start_month= int(sys.argv[2])
    end_month = int(sys.argv[4])

    filename = get_aggregate_data(year_range = range(start_year, end_year), 
                                month_range = range(start_month, end_month), 
                                agg_type = 'mean', 
                                min_population_threshold = 1000.0)

    logger.info("File Name: {}".format(filename))

    start_time = time.time()

    sc = SparkContext.getOrCreate()

    # Fix seed 
    random.seed(42) # 8,12,42

    # Agency ID
    # LA, Chicago, Houston, Harris, Phoenix, Las vegas, Philadelphia, San Antonio, San diego, Suffolk
    ids_to_check = ['CA01942', 'ILCPD00', 'TXHPD00','TX10100', 'AZ00723', 'NV00201', 'PAPEP00', 'TXSPD00', 'CA03711', 'NY05101']

    # =======================================
    #                 TASK II.A
    # =======================================
    agency_rdd = sc.textFile(filename, 32)
    agency_rdd = agency_rdd.mapPartitions(lambda x: csv.reader(x))

    header = sc.broadcast(agency_rdd.first()) # Broadcast Header
    agency_rdd = agency_rdd.filter(lambda x: x != header.value) # Extract Header

    def reFormat(partitionData):
        for row in partitionData:
            setRow = set(map(lambda x: "{}:{}".format(x[0], round(float(x[1]),2)), 
                             set(filter(lambda x: (x[1] != '') and # '__index' not in x[0]) and
                                                    round(float(x[1]),2) not in [0, 0.0, 0.00], #(x[1] not in ['0', '0.0']), 
                                                    zip(header.value[4:],row[4:])))))
            yield (row[0], setRow)

    agency_rdd = agency_rdd.mapPartitions(reFormat)

    # # CHECKPOINT II.A
    # logger.info("----------------------------------------------CHECKPOINT II.A----------------------------------------------")
    # for id in ids_to_check:
    #     logger.info("Agency ORI Code:{}:\nFeatures:\n{}".format(id, pformat(agency_rdd.lookup(id)))) 

    # =======================================
    #                 TASK II.B
    # =======================================
    shingles = agency_rdd.values() \
                        .map(lambda x: (1,x)) \
                        .reduceByKey(lambda x,y: x.union(y)) \
                        .values() \
                        .collect()[0]
    shingles_rdd = sc.parallelize(shingles) # all possible distinct shingles

    # Max hashed shingle when hashed to 32 bit integer
    maxShin = shingles_rdd.map(lambda x: binascii.crc32(str.encode(x)) & 0xffffffff).max() 

    c_prime = 4294578713 # Next Prime just higher than max shingle hash - to avoid collisions
    
    hash_num = 100 # total hash functions
    
    def genRandomPrix(hash_num):
        '''
        Generates (hash_num) random coefficients required for hashing
        '''
        coeffs = []
        for _ in range(hash_num):
            randPrix = random.randint(1, maxShin)
            while randPrix in coeffs:
                randPrix = random.randint(1, maxShin)
            coeffs.append(randPrix)
        return coeffs

    # broadcast hash function params
    a_coeffs = sc.broadcast(genRandomPrix(hash_num))
    b_coeffs = sc.broadcast(genRandomPrix(hash_num))
    c = sc.broadcast(c_prime)

    shingle_hash_rdd = shingles_rdd.map(lambda x: (x, [(i+1, (a_coeffs.value[i] * (binascii.crc32(str.encode(x)) & 0xffffffff)
                                                        + b_coeffs.value[i]) % c.value)
                                                        for i in range(hash_num)]))
    
    rdd2 = agency_rdd.flatMapValues(lambda x:x).map(lambda x:(x[1],x[0]))
    # ((i, Set_i), hashcode)
    signature_rdd = shingle_hash_rdd.join(rdd2) \
                                    .values() \
                                    .map(lambda x: (x[1], x[0])) \
                                    .reduceByKey(lambda x,y: x + y) \
                                    .flatMapValues(lambda x:x) \
                                    .flatMap(lambda x: [((x[0],x[1][0]), x[1][1])]) \
                                    .reduceByKey(lambda x, y : min(x,y)) # MinHash happens here

    # CHECKPOINT II.B
    # logger.info("----------------------------------------------CHECKPOINT II.B----------------------------------------------")
    #(Set_i, (i, hashcode)) - Convinient for lookup/filtering
    # lookup_rdd = signature_rdd.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
    #                           .filter(lambda x: x[0] in ids_to_check)
    # for id in ids_to_check:
    #     logger.info("Agency ORI Code:{}:\nSignature:\n{}".format(id, pformat(sorted(lookup_rdd.lookup(id)))))

    # =======================================
    #                 TASK II.C
    # ======================================= 
    signature_rows = 100 # Total rows in signature matrix
    b = 33 # Bands
    r = 3  # Rows
    # assert b*r == signature_rows TODO: Turn this on when using (25,4), (20,5), (10,10) etc.

    # broadcast hash function params
    sig_a_coeffs = sc.broadcast(genRandomPrix(b))
    sig_b_coeffs = sc.broadcast(genRandomPrix(b))
    sig_c = sc.broadcast(c_prime)

    sorted_sign = signature_rdd.sortBy(lambda x:x[0][1]) # sort signature matrix entries to split it into b bands 

    pairs = []
    for i in range(1,signature_rows,r):
        band_rdd = sorted_sign.filter(lambda x: x[0][1] in range(i,i+r)) # i(th) band
        all_pairs = band_rdd.map(lambda x: (x[0][0], [(x[0][1], x[1])])) \
                            .reduceByKey(lambda x, y: x+y) \
                            .mapValues(lambda x: (sig_a_coeffs.value[math.floor(i/r)] * 
                                                  int(''.join([str(m[1]) for m in sorted(x)])) + 
                                                  sig_b_coeffs.value[math.floor(i/r)]) % # Hashing Columns here
                                                  sig_c.value) \
                            .map(lambda x:(x[1],[x[0]])) \
                            .reduceByKey(lambda x,y:x+y) \
                            .filter(lambda x:len(x[1]) > 1) \
                            .mapValues(lambda x: [tuple(sorted((a,b))) for idx, a in enumerate(x) for b in x[idx + 1:]]) \
                            .values() # Create candidate pairs 
        pairs.extend([p for pairPerHash in all_pairs.collect() for p in pairPerHash])
    final_pairs = sc.parallelize(set(pairs))

    # CHECKPOINT II.C
    # logger.info("----------------------------------------------CHECKPOINT II.C----------------------------------------------")

    def calcJaccardSim(set1, set2):
        '''
        Calculates Jaccard Similairty for given two sets
        '''
        # logger.info((set1,set2))
        return len(set1.intersection(set2)) / len(set1.union(set2))

    #TOP N similar matches
    topN = 20

    for id in ids_to_check:
        
        similarity_pairs = sorted(final_pairs.filter(lambda x: x[0] == id or x[1] == id).collect())[:30]
        target_agencies = [j if i==id else i for i,j in similarity_pairs]
        sig_rdd = signature_rdd.map(lambda x: (x[0][0], (x[0][1], x[1]))).filter(lambda x: x[0] in target_agencies+[id])

        # Calculate Jaccard Similarity for matched pairs
        jaccardSims = [calcJaccardSim(set(sc.parallelize(sig_rdd.lookup(i)).values().collect()),
                                      set(sc.parallelize(sig_rdd.lookup(j)).values().collect())) 
                                        for i, j in similarity_pairs]
        # Sort in descending order
        topN_matches = sorted(list(zip(similarity_pairs, jaccardSims)), key = lambda x: x[1], reverse = True)[:topN]

        logger.info("-----------------------------FBI AGENCY:{} MATCHES AND JACCARD SIMILARITY-----------------------------".format(id))
        logger.info("Agency ORI Code:{}:\nSimilar Pairs and their Jaccard Similarity:\n{}".format(id, pformat(topN_matches)))
        # top_target_agencies = [i[1] if i[0]==id else i[0] for i,_ in topN_matches]
        # for tid in top_target_agencies:
        #     logger.info("Target Agency ORI Code:{}:\nSignature[:10]:\n{}".format(tid, pformat(sorted(sig_rdd.lookup(tid))[:10])))