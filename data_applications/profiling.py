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


# Create models from data
def best_fit_distribution(data, bins=200, ax=None):
    """ find_dist calls this function to find best fitting distribution to data"""
    # Get histogram of original data
    y, x = np.histogram(data, bins=bins, density=True) #density is true normalizes data
    x = (x + np.roll(x, -1))[:-1] / 2.0

    # Distributions to check
    DISTRIBUTIONS = [
    st.weibull_min,st.weibull_max,
        st.lognorm,
        st.norm,
        st.gamma,
        st.johnsonsu, #Johnson SU distribution
        st.burr
        #,st.fisk, st.genextreme,st.rdist, st.t,
        #may need to uncomment
        #st.cauchy,st.loglaplace,
        #st.gumbel_r, st.genlogistic
    ]

# Best holders
    best_distribution = st.norm
    best_params = (0.0, 1.0)
    best_sse = np.inf

    # Estimate distribution parameters from data
    for distribution in DISTRIBUTIONS:

        # Try to fit the distribution
        try:
            # Ignore warnings from data that can't be fit
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore')

                # fit dist to data
                params = distribution.fit(data)

                # Separate parts of parameters
                arg = params[:-2]
                loc = params[-2] #mean
                scale = params[-1]#standard dev

                # Calculate fitted PDF and error with fit in distribution
                pdf = distribution.pdf(x, loc=loc, scale=scale, *arg)
                sse = np.sum(np.power(y - pdf, 2.0))

                # if axis pass in add to plot
                try:
                    if ax:
                        pd.Series(pdf, x).plot(ax=ax)
                    end
                except Exception:
                    pass

                # identify if this distribution is better using residual summ of squares
                if best_sse > sse > 0:
                    best_distribution = distribution
                    best_params = params
                    best_sse = sse

        except Exception:
            pass

    return (best_distribution.name, best_params)

def make_pdf(dist, params, size=10000):
    """Generate distributions's Probability Distribution Function (called in find_dist)"""

    # Separate parts of parameters
    arg = params[:-2]
    loc = params[-2]
    scale = params[-1]

    # Get same start and end points of distribution
    start = dist.ppf(0.01, *arg, loc=loc, scale=scale) if arg else dist.ppf(0.01, loc=loc, scale=scale)
    end = dist.ppf(0.99, *arg, loc=loc, scale=scale) if arg else dist.ppf(0.99, loc=loc, scale=scale)

    median=dist.median(*arg, loc=loc, scale=scale)
    interval=dist.interval(0.5,*arg, loc=loc, scale=scale) #alpha is 0.5 gives us the interquartile range of data ie middle 50%

    # Build PDF and turn into pandas Series
    x = np.linspace(start, end, size)
    y = dist.pdf(x, loc=loc, scale=scale, *arg)
    pdf = pd.Series(y, x)

    return pdf,median,interval



def find_dist(data,site,plots=True):
    """Fits a list of distirbutions to the provided data and identifies the best distribution
    and then records statistics from this distribution

    Parameters
    ----------
    data : DataFrame
        DF with data from the specifc site during "night" hours (12am-4am), and the specified period
    site : string
        building name
    plots : True or False
        If True plots are generated

    Returns
    -------
    Updates prob_dist_stats and prob_dist_details dictionary

    """
    data=data[np.isfinite(data)]

    if plots==True:
        plt.figure(figsize=(12,5))
        ax = data.plot(kind='hist', bins=50, density=True, alpha=0.5)#, color=plt.rcParams['axes.color_cycle'][1])
        # Save plot limits
        dataYLim = ax.get_ylim()

        # Find best fit distribution
        best_fit_name, best_fit_params = best_fit_distribution(data, 200, ax)
        best_dist = getattr(st, best_fit_name)

        # Update plots
        ax.set_ylim(dataYLim)
        ax.set_title(u'Device Count Data \n All Fitted Distributions for '+' '+site)
        ax.set_xlabel(u'Device Count')
        ax.set_ylabel('Frequency')

        # Make PDF with best params
        pdf, median,interval = make_pdf(best_dist, best_fit_params)

        # Display
        plt.figure(figsize=(12,5))
        ax = pdf.plot(lw=2, label='PDF', legend=True)
        data.plot(kind='hist', bins=50, density=True, alpha=0.5, label='Data', legend=True, ax=ax)

        param_names = (best_dist.shapes + ', loc, scale').split(', ') if best_dist.shapes else ['loc', 'scale']
        param_str = ', '.join(['{}={:0.2f}'.format(k,v) for k,v in zip(param_names, best_fit_params)])
        dist_str = '{}({})'.format(best_fit_name, param_str)


        #prob_dist_details[site]=[best_fit_name, best_fit_params]
        #prob_dist_stats[site]=[median, interval]

        ax.set_title(u'Device Count Data with best fit distribution \n for '+site+' '+ dist_str)
        ax.set_xlabel(u'Device Count')
        ax.set_ylabel('Frequency')

    else:
        ax = data.plot(kind='hist', bins=50, density=True, alpha=0.5)#, color=plt.rcParams['axes.color_cycle'][1])
        best_fit_name, best_fit_params = best_fit_distribution(data, 200, ax)
        best_dist = getattr(st, best_fit_name)
        print("best_fit_params",best_fit_params)
        pdf,median,interval = make_pdf(best_dist, best_fit_params)
        prob_dist_details[site]=[best_fit_name, best_fit_params]
        prob_dist_stats[site]=[median, interval]
        plt.close()

    #return pdf, median,interval

def hourly_profile(df):
    """provides the hourly distribution profile of device count 

    Parameters
    ----------
    df : Device Count DataFrame
        DF at the building level
        
    Returns
    -------
    Dataframe where each row is a average profile description of the device count
    """
    occ_desc = df.groupby([df.index.hour]).describe()
    
    hour_day = list(occ_desc.index)
    hour_median = list(occ_desc[('Building Device Count',   '50%')])
    hour_25 = list(occ_desc[('Building Device Count',   '25%')])
    hour_75 = list(occ_desc[('Building Device Count',   '75%')])
    
    # plot initalizing
    font = {'size'   : 12}
    plt.rc('font', **font)
    
    fig, ax = plt.subplots(figsize=(12,10))
    plt.grid(axis = 'y', linestyle = '--', linewidth = 0.5)
    ax.bar(hour_day, hour_median, bottom=hour_25, label='Q1-Median') 
    ax.bar(hour_day, hour_75, bottom=hour_median, label='Median-Q3')

    ax.set_ylabel('Device Count')
    ax.set_xlabel('Hour of Day')
    ax.set_title('Distribution of Device Count by Hour')
    ax.set_xticks(hour_day)
    ax.legend()
    plt.show()
    
    return occ_desc