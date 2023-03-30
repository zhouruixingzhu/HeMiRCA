

import os
import json
import csv
import pandas as pd
import numpy as np
import math
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
from scipy.stats import kendalltau
from scipy.stats import spearmanr


def get_time_interval(file):
    with open(file, 'r') as f:
        data = json.load(f)
    time_interval_pairs = []
    begin = data['start']
    last_end = data['end']
    i = 1
    for fault in data['faults']:
        start = int(fault['start']) - 100
        end = int(fault['start'] + fault['duration']) + 100
        if i == 1:
            start = int(begin)
        if i == 9:
            end = int(last_end)
        time_interval_pairs.append((start, end))
        i = i + 1
    #print(time_interval_pairs)
    return time_interval_pairs

'''
index = input("chunk number: ")
directory = "./" + index
time_interval_pairs = get_time_interval(directory + '/fault_injection_' + index + '.json') 
'''


def pre_data_process(directory, metric_service_name, start_time, end_time):
    df1 = pd.read_csv(score_filepath)
    df2 = pd.read_csv(directory + metric_service_name, encoding = "ISO-8859-1")

    # normalization
    scaler = StandardScaler()
    df2[["cpu_usage_system", "cpu_usage_total", "cpu_usage_user", "memory_usage", "memory_working_set", "rx_bytes", "tx_bytes"]] = scaler.fit_transform(df2[["cpu_usage_system", "cpu_usage_total", "cpu_usage_user", "memory_usage", "memory_working_set", "rx_bytes", "tx_bytes"]])
    df = pd.merge(df1, df2, on='timestamp', how='inner')
    df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
    df = df.sort_values(by='timestamp')
    df = df.fillna(0)
    return df



def post_data_process(result, metric_service_name, corr_method, folder):
    file = metric_service_name
    path = os.path.join(folder, file)
    os.makedirs(folder, exist_ok=True)

    with open(path, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'cpu_usage_system', 'cpu_usage_total', 'cpu_usage_user', 'rx_bytes', 'tx_bytes']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for key, value in result.items():
            row = {'timestamp': key}
            for i, item in enumerate(value):
                if math.isnan(item):
                    row[fieldnames[i + 1]] = 0
                else:
                    row[fieldnames[i + 1]] = item
            writer.writerow(row)

    result_df = pd.read_csv(path)
    return result_df


def comp_pearson_correlation(window_size, metric_service_name, folder):
    df = pre_data_process(directory, metric_service_name)

    result = {}
    for i in range(len(df) - window_size):
        score = df['score'][i:i + window_size]
        cpu_usage_system = df['cpu_usage_system'][i:i + window_size]
        cpu_usage_total = df['cpu_usage_total'][i:i + window_size]
        cpu_usage_user = df['cpu_usage_user'][i:i + window_size]
        rx_bytes = df['rx_bytes'][i:i + window_size]
        tx_bytes = df['tx_bytes'][i:i + window_size]
        
        corr_cpu_usage_system, _ = pearsonr(score, cpu_usage_system)
        corr_cpu_usage_total, _ = pearsonr(score, cpu_usage_total)
        corr_cpu_usage_user, _ = pearsonr(score, cpu_usage_user)
        corr_rx_bytes, _ = pearsonr(score, rx_bytes)
        corr_tx_bytes, _ = pearsonr(score, tx_bytes)
        
        result[df['timestamp'][i + window_size]] = [corr_cpu_usage_system, corr_cpu_usage_total, corr_cpu_usage_user, corr_rx_bytes, corr_tx_bytes]
        #print(result)

    post_data_process(result, metric_service_name, 'pearson', folder)





#spearman method
def comp_spearman_global_correlation(metric_service_name, folder, start, end, fault_index):
    df = pre_data_process(directory, metric_service_name, start, end)
    result = {}
    for column in df.columns[2:]:
        result[column] = abs(df['score'].corr(df[column], method='spearman'))     
    result_df = pd.DataFrame(list(result.items()), columns=['metric', 'correlation'])
    result_df['microservice'] = metric_service_name[:-4]
    result_df['fault_index'] = fault_index
    return result_df

#kendall method
def comp_kendall_global_correlation(metric_service_name, folder, start, end, fault_index):
    df = pre_data_process(directory, metric_service_name, start, end)
    result = {}
    for column in df.columns[2:]:
        result[column] = abs(df['score'].corr(df[column], method='kendall'))     
    result_df = pd.DataFrame(list(result.items()), columns=['metric', 'correlation'])
    result_df['microservice'] = metric_service_name[:-4]
    result_df['fault_index'] = fault_index
    return result_df



def main(directory):
    #method = input("choose method (1:Pearson, 2:Kendall, 3:global, 4:sparman): ")
    #window_size = int(input("window size: "))
    df = pd.DataFrame()
    for metric_service_name in os.listdir(directory):
        if metric_service_name[-4:] == '.csv' and metric_service_name[:2] == 'ts':
            print(metric_service_name)                       
            folder = "./" + index + "/global"               
            time_interval_pairs = get_time_interval("./" + index + '/fault_injection_' + index + '.json')
            merged_df = pd.DataFrame()
            fault_index = 1
            for start, end in time_interval_pairs:
                result_df = comp_spearman_global_correlation(metric_service_name, folder, start, end, fault_index)           
                merged_df = pd.concat([merged_df, result_df], ignore_index=True)
                fault_index = fault_index + 1
            df = pd.concat([merged_df, df], ignore_index=True)
    df.sort_values(by=['fault_index', 'correlation'], ascending=[True, False], inplace=True)
    #df.to_csv(folder + '_correlaton_result.csv', index=False)
    df.to_csv(folder + '_Spearman_correlaton_result.csv', index=False)



#index = input("chunk number: ")

chunks = ['01','02','03','04','05','06','07','08','09']
for index in chunks:
    score_filepath = "./" + index + "/score.csv"
    directory = "./" + index + "/metrics/"
    main(directory)



