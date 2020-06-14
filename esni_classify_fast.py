#!/usr/bin/python3

# Spark imports
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import Row, SparkSession
from pyspark.ml.feature import VectorAssembler
import pyspark.ml.classification
from pyspark.sql.types import FloatType, StringType
from pyspark.sql.functions import udf

# Python imports
import cachetools
import re
import os
import operator
import json
import pyasn
import operator
import numpy as np
import pandas as pd
import gc
import pickle
import zlib
import pdb
import argparse
import datetime

# Generic sklearn imports
from sklearn import preprocessing
from sklearn.metrics import classification_report

# Classifiers sklearn imports
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import SGDClassifier
from sklearn.multiclass import OneVsRestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import LinearSVC
from sklearn.naive_bayes import GaussianNB

# XGBoost
try:
    from xgboost import XGBClassifier
except ImportError:
    print("XGBClassifier not imported as not installed")

parser = argparse.ArgumentParser()

# INPUT
parser.add_argument('--LOG_TCP_TRAIN_IN', type=str,
                    default='/data/DET/PDF/2018/04-Apr/2018_04_02_15_*/log_tcp_complete.gz')
parser.add_argument('--LOG_TCP_TEST_IN', type=str,
                    default='/data/DET/PDF/2018/04-Apr/2018_04_02_15_*/log_tcp_complete.gz')
parser.add_argument('--ASN_FILE', type=str)

# OUTPUT
parser.add_argument('--REPORT_OUT', type=str, default="/tmp/report.json")
parser.add_argument('--MODEL_OUT', type=str, default=None)

# PARAMETERS
parser.add_argument('--DEBUGGER', action='store_true')
parser.add_argument('--FILTER_NAME', action='store_true')
parser.add_argument('--CLASSIFIER', type=str, default=None)
parser.add_argument('--SCALER', action='store_true')
parser.add_argument('--USE_SLD', action='store_true')
parser.add_argument('--ONLY_TLS', action='store_true')
parser.add_argument('--CLASSIFY_SPARK', action='store_true')
parser.add_argument('--MIN_OCCURENCES', type=int, default=1000)
parser.add_argument('--MAX_DOMAINS', type=int, default=None)
parser.add_argument('--TRAIN_HASHING', type=str, default=None)
parser.add_argument('--TEST_HASHING', type=str, default=None)
parser.add_argument('--RIB_FILE', type=str, default="my_rib.pyasn")
parser.add_argument('--BINARY', type=str, default=None)
parser.add_argument('--DATASET_DIR', type=str, default=None)
parser.add_argument('--CLIENT_BLACKLIST', type=str, default=None)
parser.add_argument('--FEATURES', type=str, default="c_port:c_pkts_all:c_rst_cnt:c_ack_cnt:c_ack_cnt_p:c_bytes_uniq:c_pkts_data:c_bytes_all:c_pkts_retx:c_bytes_retx:c_pkts_ooo:c_syn_cnt:c_fin_cnt:s_port:s_pkts_all:s_rst_cnt:s_ack_cnt:s_ack_cnt_p:s_bytes_uniq:s_pkts_data:s_bytes_all:s_pkts_retx:s_bytes_retx:s_pkts_ooo:s_syn_cnt:s_fin_cnt:durat:c_first:s_first:c_last:s_last:c_first_ack:s_first_ack:c_rtt_avg:c_rtt_min:c_rtt_max:c_rtt_std:c_rtt_cnt:c_ttl_min:c_ttl_max:s_rtt_avg:s_rtt_min:s_rtt_max:s_rtt_std:s_rtt_cnt:s_ttl_min:s_ttl_max:c_f1323_opt:c_tm_opt:c_win_scl:c_sack_opt:c_sack_cnt:c_mss:c_mss_max:c_mss_min:c_win_max:c_win_min:c_win_0:c_cwin_max:c_cwin_min:c_cwin_ini:c_pkts_rto:c_pkts_fs:c_pkts_reor:c_pkts_dup:c_pkts_unk:c_pkts_fc:c_pkts_unrto:c_pkts_unfs:c_syn_retx:s_f1323_opt:s_tm_opt:s_win_scl:s_sack_opt:s_sack_cnt:s_mss:s_mss_max:s_mss_min:s_win_max:s_win_min:s_win_0:s_cwin_max:s_cwin_min:s_cwin_ini:s_pkts_rto:s_pkts_fs:s_pkts_reor:s_pkts_dup:s_pkts_unk:s_pkts_fc:s_pkts_unrto:s_pkts_unfs:s_syn_retx:c_pkts_push:s_pkts_push:tls_session_stat:c_last_handshakeT:s_last_handshakeT:c_appdataT:s_appdataT:c_appdataB:s_appdataB:c_msgsize_count:c_msgsize1:c_msgsize2:c_msgsize3:c_msgsize4:c_msgsize5:c_msgsize6:c_msgsize7:c_msgsize8:c_msgsize9:c_msgsize10:s_msgsize_count:s_msgsize1:s_msgsize2:s_msgsize3:s_msgsize4:s_msgsize5:s_msgsize6:s_msgsize7:s_msgsize8:s_msgsize9:s_msgsize10:c_pktsize_count:c_pktsize1:c_pktsize2:c_pktsize3:c_pktsize4:c_pktsize5:c_pktsize6:c_pktsize7:c_pktsize8:c_pktsize9:c_pktsize10:s_pktsize_count:s_pktsize1:s_pktsize2:s_pktsize3:s_pktsize4:s_pktsize5:s_pktsize6:s_pktsize7:s_pktsize8:s_pktsize9:s_pktsize10:c_sit1:c_sit2:c_sit3:c_sit4:c_sit5:c_sit6:c_sit7:c_sit8:c_sit9:s_sit1:s_sit2:s_sit3:s_sit4:s_sit5:s_sit6:s_sit7:s_sit8:s_sit9:c_pkts_data:c_pkts_data_avg:c_pkts_data_std:s_pkts_data:s_pkts_data_avg:s_pkts_data_std:c_seg_cnt:c_sit_avg:c_sit_std:s_seg_cnt:s_sit_avg:s_sit_std:c_pkts_push:s_pkts_push")

globals().update(vars(parser.parse_args()))

FEATURES = FEATURES.split(":")
if CLIENT_BLACKLIST is not None:
    CLIENT_BLACKLIST=set(CLIENT_BLACKLIST.split(":"))
else:
    CLIENT_BLACKLIST=set()

def main():
    global spark

    conf = (SparkConf()
             .setAppName("Enc SNI classification")
             .set("spark.dynamicAllocation.enabled", "false")
             .set("spark.task.maxFailures", 128)
             .set("spark.yarn.max.executor.failures", 128)
             .set("spark.executor.cores", "8")
             .set("spark.executor.memory", "12G")
             .set("spark.executor.instances", "80")
             .set("spark.network.timeout", "300")
             .set("spark.executorEnv.PYTHON_EGG_CACHE", "./.python-eggs-cache/")
             .set("spark.executorEnv.PYTHON_EGG_DIR", "./.python-eggs/")
             .set("spark.driverEnv.PYTHON_EGG_CACHE", "./.python-eggs-cache/")
             .set("spark.driverEnv.PYTHON_EGG_DIR", "./.python-eggs/")
             .set("spark.driver.maxResultSize", "1024G")   
             .set("spark.kryoserializer.buffer.max value", "10240G")           
             .set("spark.kryoserializer.buffer.max.mb",     "2047")
    )

    if not DEBUGGER:
        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")
        spark = SparkSession(sc)
        
    # Load ASNs
    print_box ("Computing ASNs")
    asns={}
    print("Target ASNs:")
    for as_name, as_nb in [ l.split(",") for l in open(ASN_FILE, "r").read().splitlines() ]:
        this_asns = as_nb.split(':')
        asns[as_name] = this_asns
        print("   {}: {}".format(as_name, as_nb))
        
    # Compute entries and ASs
    training = spark.sparkContext.textFile(LOG_TCP_TRAIN_IN)\
                    .mapPartitions(lambda p: get_tcp_entries(p, TRAIN_HASHING, asns) ).filter(lambda e: e["c_ip"] not in CLIENT_BLACKLIST)    
    testing  = spark.sparkContext.textFile(LOG_TCP_TEST_IN )\
                    .mapPartitions(lambda p: get_tcp_entries(p, TEST_HASHING, asns) ).filter(lambda e: e["c_ip"] not in CLIENT_BLACKLIST)

    # Persist and print size                    
    training.persist(StorageLevel(True, True, False, False, 1))
    testing.persist(StorageLevel(True, True, False, False, 1)) 
    print ("Training log entries:", training.count())
    print ("Testing log entries:",  testing.count())
            
    # Start classification
    print_box ("Working on classification")
    models={}
    reports={}
    for as_name in asns:
        print("Working on : {}".format(as_name))
        
        # Filter, persist and print size
        this_training = training.filter(lambda e: e["s_asn_name"]==as_name)
        this_testing  = testing.filter(lambda e: e["s_asn_name"]==as_name)
        this_training.cache()
        this_testing.cache()
                               
        training_count = this_training.count()
        testing_count  = this_testing.count()
        print ("    Training set:", training_count)
        print ("    Testing set:", testing_count)

        # Proceed only if having data points
        if training_count > 0 and testing_count > 0:

            # Compute target domains
            if BINARY is None:
                print ("    Computing Occurrences")
                occurrences = dict(this_training.map(lambda w: (w["domain"],None) ).countByKey())
                target_domains = [ k for k,v in sorted(occurrences.items(), key=operator.itemgetter(1),
                                                       reverse = True) if    v >= MIN_OCCURENCES]
                if MAX_DOMAINS is not None:
                    target_domains = target_domains[:MAX_DOMAINS]
                target_domains_dict = { d : i for i, d in enumerate (target_domains) }
                for d in occurrences:
                    if d in target_domains: 
                        print("        ", d, ":", occurrences[d])
            else:
                print("    Using binary classification with target:", BINARY)
                target_domains=[BINARY]
                target_domains_dict={BINARY:0}

            if len(target_domains) > 0: 
                # Extract features
                print ("    Extracting Features")

                training_features = this_training\
                                    .map(lambda w: extract_feature(w, target_domains, target_domains_dict)).toDF()
                testing_features  = this_testing\
                                    .map(lambda w: extract_feature(w, target_domains,target_domains_dict)).toDF()

                # Classify
                if CLASSIFY_SPARK:
                    print ("    Classifying")
                    model, training_report, testing_report  = classify_spark(training_features, testing_features,
                                                                             target_domains, target_domains_dict)

                else:
                    training_local = training_features.toPandas()
                    testing_local = testing_features.toPandas()
                    
                    if DATASET_DIR is not None:
                        if not os.path.exists(DATASET_DIR):
                            os.makedirs(DATASET_DIR)
                        training_local.to_csv("{}/{}.training.csv".format(DATASET_DIR, as_name) , index=False)
                        testing_local.to_csv("{}/{}.testing.csv".format(DATASET_DIR, as_name) , index=False)

                    print ("    Classifying")
                    model, training_report, testing_report = classify_local (training_local, testing_local)

                # Store reports
                report = {"training": training_report, "testing": testing_report}
                print ("        Macro avg F1:", testing_report["macro avg"]["f1-score"])
                print ("        Weighted avg F1:", testing_report["weighted avg"]["f1-score"])
                reports[as_name] = report
                if MODEL_OUT is not None:
                    models[as_name] = model
            else:
                print("    Skipping as no domain has minimum occurrences")
                reports[as_name] = {"error" : "no domain has minimum occurrences"}
                models[as_name] = {"error" : "no domain has minimum occurrences"}
        else:
            print("    Skipping as empty")
            reports[as_name] = {"error" : "empty dataset"}
            if MODEL_OUT is not None:
                models[as_name] = {"error" : "empty dataset"}
        gc.collect()

    # Save results on disk
    if REPORT_OUT is not None:
        json.dump(reports, open(REPORT_OUT, "w"), indent=4)
    if MODEL_OUT is not None and not CLASSIFY_SPARK:
        pickle.dump(models, open(MODEL_OUT, "wb"))

    if DEBUGGER:
        pdb.set_trace()


def is_insteresting(s_asn, asns):
    
    for as_name in asns:
        if s_asn in asns[as_name]:
            return as_name
    return None

def get_tcp_entries(lines, hashing, asns):

    asndb = pyasn.pyasn(RIB_FILE)
    first=True
    
    @cachetools.cached(cache={})
    def get_asn(s_ip):
        try:
            return str(asndb.lookup(s_ip)[0])
        except:
            return "error"
    
    @cachetools.cached(cache={})
    def get_crc32(ip):
        return zlib.crc32( ip.encode("ascii") )
        
    @cachetools.cached(cache={})        
    def my_second_level(name):
        return second_level(name)

    @cachetools.cached(cache={})        
    def my_filter_name(name):
        return filter_name(name)

            
    if hashing is not None:
        hashing_fields  = hashing.split(":")
        hash_modulo     = int(hashing_fields[0] )
        hash_min        = int(hashing_fields[1] )
        hash_max        = int(hashing_fields[2] )

    for line in lines:
        if first:

            first=False
            fields = [ x.split(":")[0] for x in line.split("#")[-1].split()]
            fields_position = {}

            for i,f in enumerate(fields):
                fields_position[f] = i

        else:

            try:

                splitted = line.split()
                c_ip = splitted[fields_position["c_ip"]]
                s_ip = splitted[fields_position["s_ip"]]

                c_bytes_all = int(splitted[fields_position["c_bytes_all"]])
                s_bytes_all = int(splitted[fields_position["s_bytes_all"]])

                fqdn = splitted[fields_position["fqdn"]]
                sni  = splitted[fields_position["c_tls_SNI"]]
                http_hostname = splitted[fields_position["http_hostname"]] \
                                if "http_hostname" in fields_position else "-"

                name = sni if sni != "-" else http_hostname
                name = name if name != "-" else fqdn

                if USE_SLD:
                    name = my_second_level(name)

                if FILTER_NAME:
                    name = my_filter_name(name)

                time = float( splitted[fields_position["first"]] )/1000

                c_isint = splitted[fields_position["c_isint"]]
                s_isint = splitted[fields_position["s_isint"]]

                s_asn = get_asn(s_ip)

                flow_features = {"count" : 1}
                for f_name in FEATURES:
                    if f_name != "count" :
                        flow_features[f_name] = float(splitted[fields_position[f_name]]) \
                                                if f_name in fields_position else 0.0

                if hashing is not None:
                    hash_res = get_crc32(c_ip)
                    if  ( hash_res % hash_modulo) >= hash_min and ( hash_res % hash_modulo) < hash_max:
                        good_c_ip = True
                    else:
                        good_c_ip = False
                else:
                    good_c_ip = True

                if ONLY_TLS:
                    good_proto = sni != "-"
                else:
                    good_proto = True

                if c_isint == "1" and s_isint=="0" and name != "-" and \
                   c_bytes_all > 0 and s_bytes_all > 0 and good_c_ip and good_proto and \
                   is_insteresting(s_asn, asns) is not None:
                    yield {
                            "c_ip": c_ip,
                            "time": time,
                            "domain": name,
                            "s_ip": s_ip,
                            "s_asn": s_asn,
                            "s_asn_name": is_insteresting(s_asn, asns),
                            "flow_features": flow_features,
                            }
            except IndexError:
                print("Index Error!")
            except ValueError:
                print("Value Error!")


def extract_feature (w, target_domains, target_domains_dict):

    label = w["domain"] if w["domain"] in target_domains_dict else "_other"
    features = {}

    for f in  w["flow_features"]:
        features["_" + f ] = w["flow_features"][f]

    features["label"] = label

    return Row(**features)


def classify_local (training_local, testing_local):


    # Prepare features and labels
    features_training = training_local.drop(columns=['label']).values
    labels_training   = training_local['label'].values
    print ("        Dataset created")
    
    # Create model
    if CLASSIFIER is not None:
        model = eval(CLASSIFIER)
    else:
        model = RandomForestClassifier(n_jobs=-1,n_estimators=100,class_weight='balanced')

    if SCALER:
        scaler = preprocessing.StandardScaler().fit(features_training)
        features_training = scaler.transform(features_training)

    model.fit(features_training, labels_training)
    print ("        Training Done")
    
    # Use model and get performance
    pred_training = model.predict(features_training)
    print ("        Training set predicted")

    # Update global stats - Train Set
    training_report = classification_report(labels_training, pred_training, output_dict=True)
    print ("        Classification report computed - training")

    # Prepare features
    features_test = testing_local.drop(columns=['label']).values
    labels_test = testing_local['label'].values
    if SCALER:
        features_test = scaler.transform(features_test)
    print ("        Testing set created")
    pred_test = model.predict(features_test)
    print ("        Testing Done")
    
    # Update global stats - Test Set
    testing_report = classification_report(labels_test, pred_test, output_dict=True)

    if SCALER:
        model = model, scaler
    return model, training_report, testing_report


def classify_spark (training, testing, target_domains, target_domains_dict):

    # Adjust
    target_domains_dict["_other"] = len(target_domains)
    target_domains.append(["_other"])

    feature_list = [ c for c in training.columns if c.startswith("_")]

    assembler = VectorAssembler(inputCols=feature_list, outputCol="features", handleInvalid="skip")

    str2idx = udf(lambda s: float(target_domains_dict[s]), FloatType())
    idx2str = udf(lambda f: target_domains[int(f)], StringType())

    training = assembler.transform(training)
    testing = assembler.transform(testing)
    training = training.withColumn("label_idx", str2idx("label") )
    testing = testing.withColumn("label_idx", str2idx("label") )

    bins = np.zeros(len(target_domains))
    freqs = { row["label_idx"]: row["count"] for row in training.select("label_idx")\
                                                           .groupBy("label_idx").count().collect() }
    for i in freqs:
        bins[int(i)] = freqs[i]
    class_weights = np.sum(bins)/(len(bins)*bins)
    idx2cw = udf(lambda f: float(class_weights[int(f)]), FloatType())
    training = training.withColumn("weigth", idx2cw("label_idx") )

    #model = pyspark.ml.classification.DecisionTreeClassifier(labelCol="label_idx",
    #                                    featuresCol="features", predictionCol="prediction_idx")
    model = pyspark.ml.classification.LogisticRegression(labelCol="label_idx", weightCol="weigth",
                                        featuresCol="features", predictionCol="prediction_idx")

    model_fit = model.fit(training)

    training_predictions    = model_fit.transform(training)
    testing_predictions     = model_fit.transform(testing)

    training_predictions = training_predictions.withColumn("prediction", idx2str("prediction_idx") )
    testing_predictions = testing_predictions.withColumn("prediction", idx2str("prediction_idx") )

    labels_training = training_predictions.select("label").toPandas().values
    labels_test     = testing_predictions.select("label").toPandas().values

    pred_training = training_predictions.select("prediction").toPandas().values
    pred_test     = testing_predictions.select("prediction").toPandas().values

    training_report = classification_report(labels_training, pred_training, output_dict=True)
    testing_report = classification_report(labels_test, pred_test, output_dict=True)

    return model_fit, training_report, testing_report

def print_box(s):
    s = "| {} |".format(s)
    print("-" * len(s))
    print(s)
    print("-" * len(s))


bad_domains=set("co.uk co.jp co.hu co.il com.au co.ve .co.in com.ec com.pk co.th co.nz com.br com.sg com.sa \
com.do co.za com.hk com.mx com.ly com.ua com.eg com.pe com.tr co.kr com.ng com.pe com.pk co.th \
com.au com.ph com.my com.tw com.ec com.kw co.in co.id com.com com.vn com.bd com.ar \
com.co com.vn org.uk net.gr".split())

def second_level(fqdn):
    if fqdn[-1] == ".":
        fqdn = fqdn[:-1]
    names = fqdn.split(".")
    if ".".join(names[-2:]) in bad_domains:
        return get3LD(fqdn)
    tln_array = names[-2:]
    tln = ""
    for s in tln_array:
        tln = tln + "." + s
    return tln[1:].lower()

def get3LD(fqdn):
    if fqdn[-1] == ".":
        fqdn = fqdn[:-1]
    names = fqdn.split(".")
    tln_array = names[-3:]
    tln = ""
    for s in tln_array:
        tln = tln + "." + s
    return tln[1:]

def filter_name (name):

    filtered_name = name.lower()
    # Strip cloudfront.net
    #filtered_name = re.sub('[a-z0-9]+\.cloudfront.net', "X.cloudfront.net", name)
    #filtered_name = re.sub('[a-z0-9]+\.profile\..*\.cloudfront.net', "X.cloudfront.net", name)

    # Strip googlevideo.com
    #filtered_name = re.sub('---sn-.*\.googlevideo\.com',"---sn-X.googlevideo.com", filtered_name)
    #filtered_name = re.sub('---sn-.*\.c\.pack\.google\.com',"---sn-X.c.pack.google.com", filtered_name)

    # Strip digits
    #filtered_name = re.sub('\d+',"D", filtered_name)
    filtered_name = re.sub('\d+(?=[a-z_\-\.]+[\w\-]+\.[\w\-])',"D", filtered_name)
    # Strip "C"
    #filtered_name = re.sub('((?<=[-\._D])|^)[a-z](?=[-\._D])',"C", filtered_name)

    return filtered_name


main()

