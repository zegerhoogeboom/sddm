#!/usr/bin/env python

import sys
import time
import pandas as pd
import numpy as np
import csv
from blockchain_parser.blockchain import Blockchain

#connect to the bitcoin rpc
blockchain = Blockchain("/data/s1968130/.bitcoin/blocks")

#open the csv file containing tx,nin

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


    result_file_name = '/home/s1953583/mydata/result/I_' + str(j).zfill(9) + '_' + str((j+range_size if j+range_size<range_max else range_max)-1).zfill(9) + '_T_' + str(time.strftime('%y%m%d_%H%M%S'))
    fp = open(result_file_name, "wb")

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
