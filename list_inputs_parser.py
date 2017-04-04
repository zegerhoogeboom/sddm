from __future__ import print_function
import sys
from blockchain_parser.blockchain import Blockchain
import itertools

# Instantiate the Blockchain by giving the path to the directory
# containing the .blk files created by bitcoind
blockchain = Blockchain(sys.argv[1])
ofile  = open('/local/s1968130/output.csv', "wb")

txs = {}

count = 0
# n = 500
# blocks = list(itertools.islice(blockchain.get_unordered_blocks(), 0, n, 1))
blocks = blockchain.get_unordered_blocks()
for block in blocks:
    for tx in block.transactions:
        if count % 10000 == 0:
            print ("Transactions done: %i" % count)
        addresses = map(lambda x: map(lambda y: y.address, x.addresses), tx.outputs)
        txs[tx.hash] = [item for sublist in addresses for item in sublist]
        count += 1

for block in blocks:
    for tx in block.transactions:
        input_addresses = []
        for input in tx.inputs:
            if input.transaction_index > 2000:
                continue
            input_addresses.append(txs.get(input.transaction_hash, ['unknown' for x in xrange(2000)])[input.transaction_index])
        stringa = str(tx.hash) + "," + ",".join(input_addresses) + "\n"
        ofile.write(stringa)
ofile.close()
