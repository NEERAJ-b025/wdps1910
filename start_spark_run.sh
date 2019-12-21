SCRIPT=${1:-"nerl_spark.py"}
INFILE=${2:-"hdfs:///user/wdps1910/sample.warc.gz"} #Give respective input file in the HDFS
OUTFILE=${3:-"result"} #Give respective output file in the HDFS
SPARK_INSTANCE=${4:-"spark://node013.cm.cluster:7077"} #Give appropriate spark instance

spark-submit \
--master ${SPARK_INSTANCE} ${SCRIPT} ${INFILE} ${OUTFILE}

hdfs dfs -cat ${OUTFILE}"/*" > ${OUTFILE}
hadoop fs -rm -r ${OUTFILE}

