#!/usr/bin/env python

import sys
import time
import random
import pandas as pd
import numpy as np
import csv
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    nodeIndexes = [-1 for i in range(28)]
    tx = 100000
    index = 0
    failedIndexes = []
    for node in range(28):
        comm.send(index, dest = node+1)
        nodeIndexes[node] = index
        index += 1000

    print "MASTER: initialization completed!"

    while index < tx:
        status=MPI.Status()
        comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()
        if tag == 0:
            print "MASTER: node " + str(source) + " succeded!"
            if failedIndexes:
                i = failedIndexes.pop()
                comm.send(i, dest = source)
                nodeIndexes[source -1] = i
            else:
                comm.send(index, dest = source)
                nodeIndexes[source - 1] = index
                index += 1000
        elif tag == 1:
            print "MASTER: node " + str(source) + " failed!"
            failedIndexes.append(nodeIndexes[source - 1])
            print failedIndexes
            comm.send(index, dest = source)
            nodeIndexes[source - 1] = index
            index += 1000
    print "MASTER: COMPLETATO!!! a seguire gli indici ancora rimasti nel failed Indexes: "
    print failedIndexes

else:
    csv_file = open("/home/s1953583/my_scratch/Python_Scripts/csv/das3.csv")
    table = pd.read_csv(csv_file)
    full_transaction_list = table["tx"].values

    rpc_user = "lusddm"
    rpc_password = "2t2e8o"

    while 1:
        index = comm.recv(source=0)
        print str(rank) + ": ricevuto indice -> " + str(index)

        try:
            rpc_connection = AuthServiceProxy("http://%s:%s@127.0.0.1:8332"%(rpc_user, rpc_password))
        except Exception:
            print str(rank) + ": fallita connessione a rpc..."
            comm.send(index, dest = 0, tag = 1)
            continue

        transaction_list = full_transaction_list[index:index+1000]
        command = [["getrawtransaction", t, 1] for t in transaction_list]

        try:
            group_of_tx = rpc_connection.batch_(command)
        except Exception:
            print str(rank) + ": fallita esecuzione batch group_of_tx..."
            comm.send(index, dest = 0, tag = 1)
            continue

        for tx in group_of_tx:
            command = [["getrawtransaction", t['txid'], 1] for t in tx['vin']]
            vout = [t['vout'] for t in tx['vin']]
            try:
                input_transactions = rpc_connection.batch_(command)
            except Exception:
                print str(rank) + ": fallita esecuzione batch input_transactions..."
                comm.send(index, dest = 0, tag = 1)
                continue

            input_addresses = [itx[0]['vout'][itx[1]]['scriptPubKey']['addresses'][0] for itx in zip(input_transactions, vout)]
            stringa = str(tx['hash']) + "," + ",".join(input_addresses) + "\n"

            try:
                result_file_name = '/local/s1953583/results/INDEX_' + str(index).zfill(8) +'_TIME_' + str(time.strftime('%y%m%d_%H%M%S'))
                fp = open(result_file_name, "wb")
                fp.write(stringa)
            except Exception:
                print str(rank) + ": fallita scrittura file..."
                comm.send(index, dest = 0, tag = 1)
                raise
                continue
            fp.close()
            comm.send(index, dest=0, tag=0)
