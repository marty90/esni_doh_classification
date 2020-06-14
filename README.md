# eSNI/DoH Domain Classification

This is the code for the CCR paper "Does Domain Name Encryption Increase Users' Privacy?"

This tool takes as input [Tstat](http://tstat.polito.it/) TCP log files and trains ML classifiers to guess the domains each flow refers to. Given a list of input Autonomous Systems (ASes), it trains a given ML classifier for each. A configurable subset of the clients in the logs are used for training. The remaining ones are used for testing.

The output is a classification report detailing the performance of the classifiers across the different ASes and domains. Optionally, you can store as output the trained models and the pre-processed CSV files for further analyses.

## Prerequisites

This code runs on Spark 2. You need Python3 with the `pandas numpy scikit-learn pyasn` modules.  You also need (large) Tstat log files to train/test your algorithm

## Usage

Usage: `spark-submit esni_classify_fast.py [-h] [ ...arguments... ]`

The arguments for input are:
* `LOG_TCP_TRAIN_IN`: a path to the input Tstat TCP logs used for training. It is a Spark path, and, as such, it supports wildcards.
* `LOG_TCP_TRAIN_IN`: a path to the input Tstat TCP logs used for testing. It is a Spark path, and, as such, it supports wildcards.
* `ASN_FILE`: The list of ASes to consider, one per line in form of `<name>:<ASN>`.

The arguments for output are:
* `REPORT_OUT`: the output JSON file where the tool reports classification performance.
* `MODEL_OUT`: the output file where the trained classifiers are stored in pickle format.
* `DATASET_DIR`: a directory where to store the pre-processed datasets in CSV format, one file per AS.


The arguments for configuring the analysis (all of them optional) are:
* `CLASSIFIER`: the scikit-learn classifier to user that will be created with an `eval` statement. Default is Random Forest.
* `USE_SLD`: Use second level domains rather than FQDNs. Default is disabled.
* `MIN_OCCURENCES`: Minimum domain occerences to have a class. Default is 1000.
* `TRAIN_HASHING`: If set, it should be in the form of `<nb_buckets>:<min>:<max>`. It filters the input training dataset, keeping only those client IPs, making a hash of them, assigning it to `nb_buckets` buckets with a modulo operation, and keeping only those between `min` and `max` bucket. For example, you can use `100:0:50`. Default is disabled and uses all clients.
* `TESTING_HASHING`: the same as `TRAIN_HASHING` but for the test set.
* `RIB_FILE`: a `pyasn` compatible RIB to be used to associate server IPs to the ASes.
* `FEATURES`: the set of columns to use as features, separated by `:`. They must be valid columns of the Tstat log TCP.

