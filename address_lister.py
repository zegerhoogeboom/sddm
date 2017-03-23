#!/usr/bin/env python

import sys
import time
import pandas as pd
import numpy as np
import csv
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

#connect to the bitcoin rpc
rpc_user = "lusddm"
rpc_password = "2t2e8o"
rpc_connection = AuthServiceProxy("http://%s:%s@127.0.0.1:8332"%(rpc_user, rpc_password))

#open a file to store the result, time added to avoir overwrites
result_file_name = 'lister_result' + str(time.strftime('%y%m%d_%H%M%S'))
fp = open(result_file_name, "wb")

#open the csv file containing tx,nin
csv_file = open("csv/octi.csv")
table = pd.read_csv(csv_file)
full_transaction_list = table["tx"].values

range_size = 100

if len(sys.argv)==3:
    range_min = int(sys.argv[1])
    range_max = int(sys.argv[2])
else:
    range_min = 0
    range_max = 10000

#divide the work in multiple groups
for j in range(range_min, range_max, range_size):
    #take a subset of the transactions. the dimension of the subset is set with the variable range_size
    transaction_list = full_transaction_list[j:(j+range_size if j+range_size<range_max else range_max)]
    print "listing addresses for transactions: " + str(j) + "-" + str((j+range_size if j+range_size<range_max else range_max)-1) + "/" + str(range_max - 1) + "..."
    command = [["getrawtransaction", t, 1] for t in transaction_list]
    #group_of_tx contains the raw transactions, retreived from the rpc, of the subset of transactions
    group_of_tx = rpc_connection.batch_(command)

    for tx in group_of_tx:
        #define the command to retreive all the raw transactions that are input of tx (where tx is a transaction in the group_of_tx previously retreived)
        command = [["getrawtransaction", t['txid'], 1] for t in tx['vin']]
        #vout is the array of indexes for each input tx (read the official bitcoin rpc documentation for more details)
        vout = [t['vout'] for t in tx['vin']]
        #retreive the input transactions
        input_transactions = rpc_connection.batch_(command)

        #finally get the address
        input_addresses = [itx[0]['vout'][itx[1]]['scriptPubKey']['addresses'][0] for itx in zip(input_transactions, vout)]


        stringa = str(tx['hash']) + "," + ",".join(input_addresses) + "\n"
        fp.write(stringa)
fp.close()
