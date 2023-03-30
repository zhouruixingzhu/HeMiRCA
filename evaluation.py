import os
import pandas as pd
import csv
import json
import ast


def calc_max_r(start_time, end_time, file_path):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        timestamp_index = header.index("timestamp")
        metrics = header[1:4]
        #print(metrics)
        data = [row for row in reader if start_time <= int(row[timestamp_index]) <= end_time]
        print((data))
        avg = {}
        for metric in metrics:
            if len(data) == 0:
                avg[metric] = 0
            else:
                avg[metric] = sum(abs(float(row[header.index(metric)])) for row in data) / len(data)
                #avg[metric] = max((float(row[header.index(metric)])) for row in data)
            
        return max(avg.values())

"""
print(calc_max_r(1650172864, 1650173464, './01/r/ts-food-service.csv'))
"""

def get_fault_time_pairs(file):
    with open(file, 'r') as f:
        data = json.load(f)
    fault_time_pairs = []
    for fault in data['faults']:
        start = int(fault['start'])
        end = int(start + fault['duration'])
        fault_time_pairs.append((start, end))
    return fault_time_pairs


'''
fault_time_pairs = get_fault_time_pairs('fault_injection.json') 
for start, end in fault_time_pairs:print(end)
'''


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


def gen_ground_truth_file(file):
    with open(file, "r") as f:
        data = json.load(f)

    faults = data["faults"]

    with open(directory + "/ground_truth_" + index + ".csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["fault_index", "fault_duration", "fault_microservice", "fault_type", "root_cause_metric"])
        fault_index = 1
        for fault in faults:
            start = int(fault['start'])
            end = int(start + fault['duration'])
            if fault["fault"] == 'cpu_load':
                root_cause_metric = ["cpu_usage_total", "cpu_usage_user", "cpu_usage_system"]
            if fault["fault"] == 'network_delay':
                root_cause_metric = ["tx_bytes", "rx_bytes"]
            if fault["fault"] == 'network_loss':
                root_cause_metric = ["tx_bytes", "rx_bytes"]
            writer.writerow([fault_index, (start, end), fault["name"].split("_")[1], fault["fault"], root_cause_metric])
            fault_index = fault_index + 1

"""
index = input("chunk number: ")
directory = "./" + index
gen_ground_truth_file(directory + '/fault_injection_' + index + '.json') 
"""


def top_k_accuracy(ground_truth, result, k):
    ground_truth = pd.read_csv(ground_truth)
    result = pd.read_csv(result)

    fault_indexs = ground_truth['fault_index'].unique()
    accuracy = 0

    for fault_index in fault_indexs:
        true_microservice = ground_truth[ground_truth['fault_index'] == fault_index]['fault_microservice'].iloc[0]
        top_k_microservices = result[result['fault_index'] == fault_index].nlargest(k, 'correlation')['microservice']

        true_metrics = ast.literal_eval(ground_truth[ground_truth['fault_index'] == fault_index]['root_cause_metric'].iloc[0])
        top_k_metrics = result[result['fault_index'] == fault_index]['metric'].unique()[:k]

        if (true_microservice in top_k_microservices.values) and (set(true_metrics).intersection(set(top_k_metrics))):
            accuracy += 1

    accuracy /= len(fault_indexs)
    return accuracy



def test():
    index = input("chunk number: ")
    directory = "./" + index
    method_dir = input("correlation dir(such as pearson_10): ")

    fault_time_pairs = get_fault_time_pairs(directory + '/fault_injection_' + index + '.json')
    results = []
    for start, end in fault_time_pairs:
        for metric_service_name in os.listdir(directory + '/' + method_dir):
            if metric_service_name[-4:] == '.csv' and metric_service_name[:2] == 'ts':
                print(metric_service_name)
                r = calc_max_r(start, end, directory + '/' + method_dir + '/' + metric_service_name)
                results.append([(start, end), metric_service_name[:-4], r])

    df = pd.DataFrame(results, columns=['fault_duration', 'microservice', 'r'])
    df.sort_values(by=['fault_duration', 'r'], ascending=[False, False], inplace=True)
    df.to_csv(directory + '/' + method_dir + '/result_'+ index + '.csv', index=False)

    K = [1,3,5,10,20]
    ground_truth = directory + "/ground_truth_" + index + ".csv"
    result = directory + '/' + method_dir + '/result_'+ index + '.csv'
    for k in K:
        accuracy = top_k_accuracy(ground_truth, result, k)
        print('Top-{} accuracy: {:.2f}%'.format(k, accuracy * 100))




def main():
    K = [1, 5, 10, 50, 100]
    chunks = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
    total_accuracy = {k: 0 for k in K}

    for index in chunks:
        directory = "./" + index
        ground_truth = directory + "/ground_truth_" + index + ".csv"
        result = directory + '/global_kendall_correlaton_result.csv'

        for k in K:
            accuracy = top_k_accuracy(ground_truth, result, k)
            total_accuracy[k] += accuracy
            print('Top-{} accuracy: {:.2f}%'.format(k, accuracy * 100))
        
    num_chunks = len(chunks)
    for k in K:
        avg_accuracy = total_accuracy[k] / num_chunks
        print('Average Top-{} accuracy: {:.2f}%'.format(k, avg_accuracy * 100))




if __name__ == '__main__':
    main()





