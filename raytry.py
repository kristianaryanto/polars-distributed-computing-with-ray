import pandas as pd
import ray
import random,logging
import os
import polars as pl

import time
# @ray.remote(memory=500 * 1024 * 1024)
# def fibonacci_distributed(sequence_size):
#     fibonacci = []
#     for i in range(0, sequence_size):
#         if i < 2:
#             fibonacci.append(i)
#             continue
#         fibonacci.append(fibonacci[i-1]+fibonacci[i-2])
#     return sequence_size

@ray.remote(memory=500 * 1024 * 1024) # ray initialized with 500 MB
def polars_csv_read(path):
    df = pl.read_csv(path) # read csv into polars dataframe
    return df
# Ray
def run_remote(sequence_size):
    # Starting Ray
    # ray.init(num_cpus = 8,object_store_memory = 10**8,local_mode = True)
    # ray.init(num_cpus = 3,local_mode = False)
    # ray.init(num_cpus = 3,object_store_memory = 10**8,local_mode = False)
    start_time = time.time()
    # results = ray.get([fibonacci_distributed.remote(sequence_size) for _ in range(os.cpu_count())])
    results = ray.get([fibonacci_distributed.remote(sequence_size) for _ in range(10)])
    # results = ray.get(fibonacci_distributed.remote(sequence_size) )
    # print(results)
    duration = time.time() - start_time
    print('Sequence size: {}, Remote execution time: {}'.format(sequence_size, duration))  

if __name__ == '__main__':
    # time.sleep(5)
    # run_remote(900000)
    run_remote(400000)
    # run_remote(900)