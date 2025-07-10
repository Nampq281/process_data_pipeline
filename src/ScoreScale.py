import pandas as pd
import numpy as np

def cut_score(df_input, score_col, bin_cut=None, q=20):
    """
    score_col: column contain score
    bin_cut pd.IntervalIndex: predefined list of cut interval
    """
    df = df_input.copy()
    df[score_col] = df[score_col].astype(int)

    if not isinstance(bin_cut, pd.IntervalIndex):
        minSc = df[score_col].min()
        maxSc = df[score_col].max()
        df[score_col] = df[score_col].replace(minSc, 0)
        df[score_col] = df[score_col].replace(maxSc, 999)
        df['score_bin'] = pd.qcut(x=df[score_col], 
                                            q=q,
                                            precision=0,
                                            duplicates='drop'
                                          )
    else:
        df['score_bin'] = pd.cut(x=df[score_col], bins=bin_cut)
    return df['score_bin']


def distribution_tbl(score_df, score_bin_col='score_bin', label_col='label'):
    """
    score_df need to contains
        pd.IntervalIndex column for cutting score: [score_bin]
        Label column: [label]
        Score column
    """
    # Count observation
    df = pd.pivot_table(score_df, index=score_bin_col, columns=label_col, aggfunc="count").reset_index()
    df.columns = ['band', 'good', 'bad']
    df['cum_bad'] = df['bad'].cumsum()
    df['cum_good'] = df['good'].cumsum()
    df['#obs'] = df['good'] + df['bad']
    total_obs = df['#obs'].sum()

    # Calculate ratio
    df['%obs'] = round(df['#obs']/total_obs,4)
    df['cum_%obs'] = df['%obs'].cumsum()
    df['bad_rate'] = round(df['bad']/df['#obs'],4)
    # df['cum_bad_rate'] = None

    total_bad = df['bad'].sum()
    total_good = df['good'].sum()

    # Bad rate cummulative
    bad_under_band = []
    for i in range (0, len(df)):
        agg_bad = df[(i+1):]['bad'].sum()
        agg_total = df[(i+1):]['#obs'].sum()
        pc = round(agg_bad/agg_total,4)
        bad_under_band.append(pc)

    df['bad_rate_cum_under_band'] = bad_under_band
    df['cum_%bad'] = round(df['cum_bad']/total_bad,4)
    df['cum_%good'] = round(df['cum_good']/total_good,4)
    
    return df    


def score_scaling(offset, factor, event_p):
    ln_odds = np.log((1-event_p)/event_p)
    score = offset + factor*ln_odds
    score = np.round(score,0)
    return score    

def score_function(base_score, base_odds, pdo):
    factor = pdo/np.log(2)
    offset = base_score - (factor*(np.log(base_odds)))
    return offset, factor    