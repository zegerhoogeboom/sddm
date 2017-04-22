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

        self.tx_map_queue = mp.Queue()
        self.failed_tx_map_queue = mp.Queue()

        self.complete_txs = manager.dict()
        self.complete_failed_txs = manager.list()

    def create_mapping(self):
        self.blockchain = Blockchain(sys.argv[1])

        p_read_inputs = mp.Process(target=self.__read_file, args=())
        p_populate_mapping = [mp.Process(target=self.__populate_mapping, args=())
                              for i in range(self.numprocs)]

        p_combine_mappings = mp.Process(target=self.__combine_tx_mappings, args=(self.complete_txs, self.complete_failed_txs))
        p_write_tx = mp.Process(target=self.__write_tx_mappings, args=(self.complete_txs, self.complete_failed_txs))

        p_read_inputs.start()
        for p in p_populate_mapping:
            p.start()

        p_read_inputs.join()
        i = 0
        for p in p_populate_mapping:
            p.join()
            print "Done", i
            i += 1

        #
        self.tx_map_queue.put("STOP")
        self.failed_tx_map_queue.put("STOP")
        p_combine_mappings.start()
        p_combine_mappings.join()

        p_read_inputs_again = mp.Process(target=self.__read_file, args=())
        p_read_inputs_again.start()
        p_write_tx.start()
        p_read_inputs_again.join()
        p_write_tx.join()

    def __read_file(self):
        count = 0
        n = 500000
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
        for block in iter(self.input_queue.get, "STOP"):
            i += 1
            if i % 10000 == 0:
                print ("Queue size: %i" % self.input_queue.qsize())
            for tx in block.transactions:
                try:
                    addresses = map(lambda x: map(lambda y: y.address, x.addresses), tx.outputs)
                    if len(addresses) <= 1:
                        continue
                    txs[tx.hash] = [item for sublist in addresses for item in sublist]
                except Exception:
                    failed_txs.append(tx.hash)

        self.tx_map_queue.put(txs)
        self.failed_tx_map_queue.put(failed_txs)

    def __combine_tx_mappings(self, txs, failed_txs):
        txs_tmp = txs
        failed_txs_tmp = failed_txs
        for mapping in iter(self.tx_map_queue.get, "STOP"):
            txs_tmp.update(mapping)
        for mapping in iter(self.failed_tx_map_queue.get, "STOP"):
            failed_txs_tmp = failed_txs_tmp + mapping
        txs = txs_tmp
        failed_txs = failed_txs_tmp

    def __write_tx_mappings(self, txs, failed_txs):
        ofile = open('/local/s1968130/output.csv', "wb")
        failed_file = open('/local/s1968130/failed_output.csv', "wb")

        for block in iter(self.input_queue.get, "STOP"):
            for tx in block.transactions:
                input_addresses = []
                for input in tx.inputs:
                    tx_inputs = txs.get(input.transaction_hash, None)
                    if tx_inputs is None:
                        continue
                    input_addresses.append(tx_inputs[input.transaction_index])
                if len(input_addresses) <= 1:
                    continue
                stringa = str(tx.hash) + "," + ",".join(input_addresses) + "\n"
                ofile.write(stringa)

        ofile.close()
        failed_file.write("\n".join(failed_txs))
        failed_file.close()

BlockChainParser().create_mapping()