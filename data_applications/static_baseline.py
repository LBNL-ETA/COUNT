import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import matplotlib
import seaborn
import logging
import warnings
import scipy.stats as st
import statsmodels as sm
import statistics


def remove_static(df, unocc_st = '01:00:00', unocc_et = '04:00:00', quartile='75%'):
                  
    """Calculates the static devices during specified periods and removes the devices from the dataframe

    Parameters
    ----------
    df : DataFrame
        DF at the building level
    unocc_st : unoccupied start time string input
        Set to 1:00 am as standard
    unocc_et : unoccupied end time string input
        Set to 4:00 am as standard
    ** Note: this is a time range in which you believe there would most likely not be people within the builing
    quartile : string input
        quartile level of how much you would like to be removed as the baseline, ( '25%', '50%', '75%')
        
    Returns
    -------
    Dataframe where the static baseline is removed from all the device counts for each building
    """
    # plot initalizing
    font = {'size'   : 12}
    plt.rc('font', **font)
    
    # filtering df for unoccupied time range
    night_occ = df.between_time(unocc_st, unocc_et)
    # Retriving stats for unoccupied times
    removed_devices = night_occ.iloc[:,0].describe()[quartile]
    
    no_static_df = df - removed_devices
    
    no_static_df[no_static_df<0]=0
    
    kwargs = dict(alpha=0.5, bins=100)
    
    plt.figure(figsize=(10, 10), dpi=80)
    
    plt.xlim(xmax = df.iloc[:,0].describe()['75%'])
    
    plt.hist(df.iloc[:,0].values.tolist(), **kwargs, label='With Static Devices')
    plt.hist(no_static_df.iloc[:,0].values.tolist(), **kwargs, label='Without Static Devices')
    #plt.hist(df.iloc[:,0].values.tolist(), **kwargs, label='With Static Devices')
    plt.gca().set(title='Static Removal Shift', xlabel='Device Count', ylabel='Device Count Frequency')
    plt.legend()
    
    
    return no_static_df