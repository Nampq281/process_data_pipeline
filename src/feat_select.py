from optbinning import OptimalBinning
import pandas as pd
import numpy as np
from sklearn.feature_selection import f_classif, chi2
from tqdm import tqdm

def binning(df, feature, target, bin_type, **params):
    """
    bin_type str: categorical/numerical
    """
    x = df[feature]
    y = df[target]
    optb = OptimalBinning(name=feature, dtype=bin_type, **params)
    optb.fit(x, y)
    binning_table = optb.binning_table
    binning_table.build()
    return binning_table.iv, optb

def auto_bin(df, feature, target, **params):
    data_type = df[feature].dtype
    bin_type = "categorical" if data_type in ('object', 'string') else "numerical"
    iv, optb = binning(df, feature, target, bin_type=bin_type, **params)
    return iv, optb

def stat_test(df, feature, target):
    x_stat = np.array(df[feature]).reshape(-1, 1)
    y = df[target]
    data_type = df[feature].dtype
    try:
        _, test_result = f_classif(x_stat, y) if data_type == 'float' else chi2(x_stat, y)
    except:
        test_result = [None]
    return test_result

def feature_ranking(df, label, feat_list):
    stat_dict = {'feature':[],'iv':[], 'pval_test':[]}

    for feature in tqdm(feat_list):
        iv,_ = auto_bin(df, feature, label)
        test_result = stat_test(df, feature, label)

        stat_dict['feature'].append(feature)
        stat_dict['iv'].append(iv)
        stat_dict['pval_test'].append(test_result[0])

    return pd.DataFrame(stat_dict)



#-------------------------------- Auto scan --------------------------------
from optbinning import OptimalBinning

def trend_detect(tbl):
    " tbl: binning table "
    # Get trend
    index_l = tbl[~tbl['Bin'].isin(['Special', 'Missing', ''])].index.values
    q = [tbl['Event rate'][round(np.percentile(index_l, i),0)] for i in [0, 33, 66, 100]]
    if q[0]>=q[1]>=q[2]>=q[3]:
        trend = ('[-]')
    elif q[0]<=q[1]<=q[2]<=q[3]:
        trend = ('[+]')
    elif (q[0]>=q[1]) and (q[2]<=q[3]):
        trend = ('[U]')
    elif (q[0]<=q[1]) and (q[2]>=q[3]):
        trend = ('[^]')
    else:
        trend = ('No trend')
    return trend

def group_feat(feat):
    if feat in pcb_feat_columns:
        feat_type = 'PCB'
    elif feat in card_info_columns:
        feat_type = 'card Info'
    else:
        feat_type = 'old_PCB'
    return feat_type
    
def auto_bin_scan(df, feat, dtype, config_result, label='FPD1', **params):
    """
    # params {}
    # binning_table.plot(metric="event_rate", show_bin_labels=True, figsize=(6,4))
    """
    x_bin = df[feat]
    y_bin = df[label]
    optb = OptimalBinning(name=feat, dtype=dtype, **params)
    optb.fit(x_bin, y_bin)
    binning_table = optb.binning_table
    tbl = binning_table.build()

    trend = trend_detect(tbl)
    no_bins = len(tbl)-2 # including Missing
    if no_bins<=2:
        trend = 'No trend'
    # feat_type = group_feat(feat) 
    feat_type=None # <default>
    binning_table.analysis(print_output=False)
    
    config_result['feat_type'].append(feat_type)
    config_result['feat'].append(feat)
    config_result['gini'].append(binning_table.gini)
    config_result['iv'].append(binning_table.iv)
    config_result['js'].append(binning_table.js)
    config_result['qa'].append(binning_table.quality_score)
    config_result['binning_table'].append(binning_table)
    config_result['no_bins'].append(no_bins)
    config_result['trend'].append(trend)


def get_feat_result(config_result, feat):
    """
    config_result = {'feat':[], 'feat_type':[], 'gini':[], 'iv':[], 'js':[], 'qa':[], 'binning_table':[], 'no_bins':[], 'trend':[]}
    """
    ana_result = pd.DataFrame(config_result)
    filter_feat = ana_result['feat']==feat
    ana_result[filter_feat]['binning_table']\
        .values[0]\
        .plot(metric="event_rate", show_bin_labels=True, figsize=(6,4))
    
    tbl = ana_result[filter_feat]['binning_table']\
        .values[0]\
        .build()
    return tbl