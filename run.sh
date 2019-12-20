#!/usr/bin/env bash

##########################
##  Start sparql server ##
##########################

## Not needed this server now..
KB_PORT=9090
KB_BIN=/home/jurbani/trident/build/trident
KB_PATH=/home/jurbani/data/motherkb-trident

echo "Lauching an instance of the Trident server on a random node in the cluster ..."
prun -o .kb_log -v -np 1 $KB_BIN server -i $KB_PATH --port $KB_PORT </dev/null 2> .kb_node &
echo "Waiting 5 seconds for trident to set up (use 'preserve -llist' to see if the node has been allocated)"
until [ -n "$KB_NODE" ]; do KB_NODE=$(cat .kb_node | grep '^:' | grep -oP '(node...)'); done
sleep 5
KB_PID=$!
echo "Trident should be running now on node $KB_NODE:$KB_PORT (connected to process $KB_PID)"

#python3 sparql.py $KB_NODE:$KB_PORT "select * where {<http://rdf.freebase.com/ns/m.01cx6d_> ?p ?o} limit 100"

#query="select * where {\
  #?s <http://www.w3.org/2002/07/owl#sameAs> <http://rdf.freebase.com/ns/m.0k3p> .\
  #?s <http://www.w3.org/2002/07/owl#sameAs> ?o .}"
#python3 sparql.py $KB_NODE:$KB_PORT "$query"

#query="select distinct ?abstract where {  \
  #?s <http://www.w3.org/2002/07/owl#sameAs> <http://rdf.freebase.com/ns/m.0k3p> .  \
  #?s <http://www.w3.org/2002/07/owl#sameAs> ?o . \
  #?o <http://dbpedia.org/ontology/abstract> ?abstract . \
#}"
#python3 sparql.py $KB_NODE:$KB_PORT "$query"

#kill $KB_PID



##################################
##  Start Elastic Search Server ##
##################################

ES_PORT=9200
ES_BIN=$(realpath ~/elasticsearch-2.4.1/bin/elasticsearch)

>.es_log*
prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
until [ -n "$(cat .es_log* | grep YELLOW)" ]; do sleep 1; done
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"

#python3 elasticsearch.py $ES_NODE:$ES_PORT "Vrije Universiteit Amsterdam"

#kill $ES_PID


#######################
## Run Python script ##
#######################

python3 main.py "hdfs://127.0.0.1:9000/user/neeraj/sample.warc.gz" "sample_result" $ES_NODE:$ES_PORT $KB_NODE:$KB_PORT





