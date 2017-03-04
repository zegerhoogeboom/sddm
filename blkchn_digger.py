#!/usr/bin/env python

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import csv

rpc_user = "lusddm"
rpc_password = "2t2e8o"

rpc_connection = AuthServiceProxy("http://%s:%s@127.0.0.1:8332"%(rpc_user, rpc_password))

for j in range(399001, 450000, 1000):
    print "starting range [" + str(j) + ", " + str(j+999) + "]... "
    commands = [[ "getblockhash", height] for height in range(j,j+1000)]
    block_hashes = rpc_connection.batch_(commands)

    commands = [[ "getblock", block_hash] for block_hash in block_hashes]
    block_infos = rpc_connection.batch_(commands)

    n = len(block_hashes)

    res = []

    for i in range(len(block_hashes)):
        txs = block_infos[i]['tx']
        commands = [[ "getrawtransaction", tx, 1 ] for tx in txs]
        tx_info = rpc_connection.batch_(commands)

        for t in tx_info:
            if(len(t['vin']) > 1):
                res.append((t['txid'], len(t['vin'])))

    filename = 'csv/test' + str(j) + '.csv'
    csv_file = open(filename, "wb")
    writer = csv.writer(csv_file)
    for row in res:
        writer.writerow(row)
