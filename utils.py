
import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def read_json(filepath):
    if os.path.exists(filepath):
        assert filepath.endswith('.json')
        with open(filepath, 'r') as f:
            return json.loads(f.read())
    else: 
        logging.raiseExceptions("File path "+filepath+" not exists!")
        return


def get_start_end(filepath):
    trace_json = read_json(filepath)
    sendingTime = []
    for key in trace_json.keys():
        sendingTime.append(int(key))
    print(len(sendingTime))
    start = min(sendingTime)
    end = max(sendingTime)
    print("start:",start, ",end:",end)
    return start, end


def combine_files(dir_path):

    sub_dirs = [d for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d))]
    
    content = []
    
    for sub_dir in sub_dirs:
        sub_dir_path = os.path.join(dir_path, sub_dir)
        normal_file_path = os.path.join(sub_dir_path, 'abnormal.txt')
        #normal_file_path = os.path.join(sub_dir_path, 'ground_truth_' + sub_dir_path.split('/')[1] + '.csv')

        if os.path.exists(normal_file_path):
            with open(normal_file_path, 'r') as normal_file:
                content.extend(normal_file.readlines())
                
    #rank
    content.sort(key=lambda x: int(x.split(':')[0]))
    
    with open(os.path.join(dir_path, 'combined_abnormal.txt'), 'w') as f:
        for line in content:
            f.write(line)
                    
    print('done')

#combine_files('./')



import random
def separate_normal_data():
    # 读取combined_normal.txt文件
    with open("combined_normal.txt", "r") as f:
        data = f.readlines()

    random.shuffle(data)
    data_len = len(data)
    training_len = int(data_len * 0.9)

    with open("training.txt", "w") as f:
        f.writelines(data[:training_len])

    with open("test_normal.txt", "w") as f:
        f.writelines(data[training_len:])

#separate_normal_data()



def get_mean_latency():
    filename = "./04/abnormal.txt"

    with open(filename) as f:
        data = [line.strip().split(':')[1].split(',') for line in f]
        data = [list(map(float, line)) for line in data]

    num_columns = len(data[0])
    num_rows = len(data)

    column_sums = [0.0] * num_columns
    for row in data:
        for i, item in enumerate(row):
            column_sums[i] += item

    column_means = [sum_ / num_rows for sum_ in column_sums]

    for i, mean in enumerate(column_means):
        print(f'"{i + 1}": {mean}')


#get_mean_latency()


