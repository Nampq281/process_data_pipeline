import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def plot_stats(feature:str, label:str, df, horizontal_layout=True, label_rotation=False, title=None):
    #prepare data
    temp_count = df[feature].value_counts()
    df_count = pd.DataFrame({feature:temp_count.index, 'No contracts':temp_count.values})
    df_perc = df[[feature, label]].groupby([feature], as_index=False).mean()
    df_perc.sort_values(by=label, ascending=False, inplace=True)
    
    #initialize subplot
    #horizontal layout
    if horizontal_layout:
        fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(8,4))
    else:
        fig, (ax1, ax2) = plt.subplots(nrows=2, ncols=1, figsize=(7,10))
    sns.set_color_codes('pastel')
    s1 = sns.barplot(ax=ax1, x=feature, y='No contracts', order=df_perc[feature], data=df_count)
    s2 = sns.barplot(ax=ax2, x=feature, y=label, order=df_perc[feature], data=df_perc)
    
    #tickmark flip
    if label_rotation:
        s1.set_xticklabels(s1.get_xticklabels(), rotation=90)
        s2.set_xticklabels(s2.get_xticklabels(), rotation=90)
    
    #set up chart
    plt.ylabel('Event rate', fontsize=10)
    plt.tick_params(axis='both', which='major', labelsize=10)
    ax2.yaxis.set_label_position("right")
    ax2.yaxis.tick_right()
    plt.title(title)
    plt.show()

    
def plot_distribution(df, feature):
    '''
    plot continuous data hist
    '''
    plt.figure(figsize=(10,6))
    plt.title("Distribution of %s"%feature)
    sns.histplot(df[feature].dropna(), color='green', kde=True, bins=100)
    plt.show()


def plot_distribution_target(df, feature_list, label, nrows=3, bw_method=0.01):
    '''
    plot continuous data by binning with event rate in each bin
    maximum 6 figures at once
    '''
    #data
    t1 = df.loc[df[label] != 0]
    t0 = df.loc[df[label] == 0]
    #get frame
    sns.set_style('whitegrid')
    plt.figure()
    fig, ax = plt.subplots(nrows=nrows, ncols=2, figsize=(12, 6*nrows))
    
    i=0
    for feature in feature_list:
        i += 1
        plt.subplot(nrows,2,i)
        sns.kdeplot(t1[feature], bw_method=bw_method, label='TARGET [1]')
        sns.kdeplot(t0[feature], bw_method=bw_method, label='TARGET [0]')
        plt.ylabel('Density plot', fontsize=12)
        plt.xlabel(feature, fontsize=12)
        locs, labels = plt.xticks()
        plt.tick_params(axis='both', which='major', labelsize=10)
    plt.legend()
    plt.show()


import seaborn as sns

def plot_corr_matrix(df):
    corr = df.corr()
    plt.figure(figsize=(10,6))
    sns.heatmap(corr, vmax=0.8, center=0,
                square=True, linewidths=2, cmap='Blues')
    plt.tick_params(axis='both', labelsize=7)
    plt.show()