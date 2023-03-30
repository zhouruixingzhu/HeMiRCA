
import numpy as np
import pandas as pd
import os
import pickle
import json
import string
from utils import read_json


# microservices of TrainTicket
service_dict = {
    'ts-assurance-service':1, 'ts-auth-service':2, 'ts-basic-service':3, 
    'ts-cancel-service':4, 'ts-config-service':5, 'ts-contacts-service':6,
    'ts-food-map-service':7, 'ts-food-service':8, 'ts-inside-payment-service':9,
    'ts-notification-service':10, 'ts-order-other-service':11, 'ts-order-service':12,
    'ts-payment-service':13, 'ts-preserve-service':14, 'ts-price-service':15,
    'ts-route-plan-service':16, 'ts-route-service':17, 'ts-seat-service':18,
    'ts-security-service':19, 'ts-station-service':20, 'ts-ticketinfo-service':21,
    'ts-train-service':22, 'ts-travel-plan-service':23, 'ts-travel-service':24,
    'ts-travel2-service':25, 'ts-user-service':26, 'ts-verification-code-service':27
}



def get_trace_info_func(filepath, index, trace_info_path):
    print("*** Extracting key features from the raw spans data...")
    spans_json = read_json(filepath)

    trace_dict = {}
    for trace in spans_json:

        process_dict = trace["processes"]
        service_name_dict = {}
        for key in process_dict:
            service_name_dict[key] = process_dict[key]["serviceName"]

        span_list = []
        for span in trace["spans"]:
            span_data = {}
            span_data["spanID"] = span["spanID"]
            span_data["sendingTime"] = round(span["startTime"]/1000000 - 8*3600)
            span_data["duration"] = (span["duration"]/1000000)
            if span["references"]:
                span_data["parentSpanID"] = span["references"][0]["spanID"]
            else:
                # empty list
                span_data["parentSpanID"] = ""
            span_data["serviceName"] = service_name_dict[span["processID"]]
            span_data["callee"] = service_dict.get(service_name_dict[span["processID"]])
            span_data["caller"] = ""
            span_list.append(span_data)
        trace_dict[trace["traceID"]] = span_list
    
        for key in trace_dict.keys():
            for span in trace_dict[key]:
                if span["parentSpanID"] == "":
                    continue
                else:
                    for par_span in trace_dict[key]:
                        if span["parentSpanID"] == par_span["spanID"]:
                            span["caller"] = par_span["callee"]

    with open(trace_info_path, "w") as json_file:
        json.dump(trace_dict, json_file)
        print("trace_info_" + index + ".json write done!")
    return trace_dict


def get_trace_lat_func(trace_dict, index, trace_lat_path):
    print("*** trace latency computation...")
    trace_lat_dic = {}
    for key in trace_dict.keys():
        for span in trace_dict[key]:
            if span["sendingTime"] not in trace_lat_dic.keys():
                trace_lat_dic[span["sendingTime"]] = {}
            callee = str(span["callee"])
            if callee not in trace_lat_dic[span["sendingTime"]].keys():
                trace_lat_dic[span["sendingTime"]][callee] = []           
            trace_lat_dic[span["sendingTime"]][callee].append(span["duration"])

    for key in list(trace_lat_dic.keys()):
        if trace_lat_dic[key] == {}: 
            del trace_lat_dic[key]
    with open(trace_lat_path, 'w') as json_file:
        json.dump(trace_lat_dic, json_file)
        print("trace_lat_" + index + ".json write done!")
    return trace_lat_dic


def get_trace_vector(input_filepath, output_filepath, index):
    print("*** trace vectorization...")
    traces = read_json(input_filepath)
    fo = open(output_filepath, "w")
    for timestamp in traces.keys():
        var_line = str(timestamp) + ":"
        zeors_array = np.zeros(27)
        A = zeors_array
        for callee in traces[timestamp].keys():           
            latency = sum(traces[timestamp][callee])/len(traces[timestamp][callee])
            callee = int(callee)
            A[callee-1] = latency
        #print(A)
        for i in range(A.size):
            if A[i] == 0:
                var_line = var_line + str(int(A[i])) + ','
            else:
                var_line = var_line + str(float(A[i])) + ','
        var_line = var_line.strip(',')       
        #print(var_line)
        fo.writelines(var_line + "\n")
    print("trace_vector_" + index + ".txt write done!")
    fo.close()


def separate_trace_data_into_normal_and_abnormal_file(index, trace_vector_path):
    with open("./" + index + "/fault_injection_" + index + ".json", "r") as f:
        data = json.load(f)

    start = data["start"]
    end = data["end"]
    faults = data["faults"]

    abnormal_file = open("./" + index + "/abnormal.txt", "w")
    normal_file = open("./" + index + "/normal.txt", "w")

    with open(trace_vector_path, "r") as f:
        for line in f:
            timestamp = int(line.split(":")[0])
            #if timestamp >= start and timestamp <= end:
            for fault in faults:
                if fault["start"] <= timestamp <= (fault["start"] + fault["duration"]):
                    abnormal_file.write(line)
                    break
            else:
                normal_file.write(line)

    normal_file.close()
    abnormal_file.close()



if "__main__" == __name__:
    index = input("input index: ")
    raw_filepath = "./" + index + "/spans.json"
    trace_info_path = "./" + index + "/trace_info_" + index + ".json"
    trace_lat_path = "./" + index + "/trace_lat_" + index +".json"
    trace_vector_path = "./" + index + "/trace_vector_" + index + ".txt"

    #get trace_info.json
    trace_info_dict = get_trace_info_func(raw_filepath, index, trace_info_path)
    #get trace_lat.json
    trace_lat_dic = get_trace_lat_func(trace_info_dict, index, trace_lat_path)
    #get vector for VAE
    get_trace_vector(trace_lat_path, trace_vector_path, index)
    separate_trace_data_into_normal_and_abnormal_file(index, trace_vector_path)

