import itertools
import numpy as np
import multiprocessing as mp

import sys
from blockchain_parser.blockchain import Blockchain


# Basic structure taken from:
# http://stackoverflow.com/questions/2359253/solving-embarassingly-parallel-problems-using-python-multiprocessing
class BlockChainParser:
    def __init__(self):
        manager = mp.Manager()

        # Dict to share between processes.
        # More info: https://docs.python.org/3/library/multiprocessing.html#sharing-state-between-processes

        # Max amount of processors. This setting: Use ALL of the CPU's!
        self.numprocs = 32 #mp.cpu_count() # 24

        self.input_queue = mp.Queue()

    def create_mapping(self):
        self.transactions_file = open('/local/s1968130/transactions.txt', "a")
        self.blockchain = Blockchain(sys.argv[1])

        p_read_inputs = mp.Process(target=self.__read_file, args=())
        p_populate_mapping = [mp.Process(target=self.__populate_mapping, args=())
                              for i in range(self.numprocs)]
        p_read_inputs.start()
        for p in p_populate_mapping:
            p.start()

        p_read_inputs.join()
        i = 0
        for p in p_populate_mapping:
            p.join()
            print "Done", i
            i += 1

        self.transactions_file.close()

    def __read_file(self):
        count = 0
        n = 50000
        blocks = list(itertools.islice(self.blockchain.get_unordered_blocks(), 0, n, 1))
        for block in blocks:
            self.input_queue.put(block)
            if count % 10000 == 0:
                print ("Transactions done: %i" % count)
            count += block.n_transactions
        for i in range(self.numprocs):
            self.input_queue.put("STOP")

    def __populate_mapping(self):
        txs = {}
        failed_txs = []
        i = 0
        print("Going through input queue...")
        for block in iter(self.input_queue.get, "STOP"):
            i += 1
            if i % 10000 == 0:
                print ("Queue size: %i" % self.input_queue.qsize())
            for tx in block.transactions:
                try:
                    addresses = map(lambda x: map(lambda y: y.address, x.addresses), tx.outputs)
                    print ("#Addresses: %i" % len(addresses))
                    if len(addresses) <= 1:
                        continue
                    stringa = str(tx.hash) + "," + ",".join(addresses) + "\n"
                    self.transactions_file.write(stringa)
                except Exception:
                    failed_txs.append(tx.hash)

BlockChainParser().create_mapping()