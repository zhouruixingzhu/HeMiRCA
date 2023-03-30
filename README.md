# HeMiRCA

## Step 1: Check Python Dependencies
Please run:
```shell
pip install -r requirements.txt
```

## Step 2: Data Downloading 
download the data by yourself (TT or SN Dataset: https://zenodo.org/record/7615394 ) and put it in the ```./trainticket```  or ```./sn``` path. 

## Step 3: Traces Preprocessing
extract trace latency from spans.py and then construct span vectors.
```shell
python trace_preorocess.py
```

## Step 4: Trace Anomaly Scoring
Using normal span vectors to train VAE model. Calculating the anomaly scores of the testing data.
```shell
./anomaly_scoring/run.sh
```

## Step 5: Correlation Calculation and Ranking the Root-Cause Metric
```shell
python correlation_calculation.py
python evaluation.py
```