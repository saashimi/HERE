"""
Script to calculate average HERE speeds.
Calculates mean and 5th and 95th percentile speeds per AM and PM peak.
Script by Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov
Useage:
    >>> python HERE_parser.py <AM or PM>
"""

import os
import sys
import pandas as pd
import numpy as np
import datetime as dt


def group_TMC(df_tmc):
    """Groups by TMC and aggregates average speeds.
    Args: df_tmc, a pandas dataframe.
    Returns: df_tmc, a pandas dataframe grouped by TMC.
    """
    tmc_operations = ({'MEAN': 'mean',
                       'MEAN_5': lambda x: np.percentile(x, 5),
                       'MEAN_95': lambda x: np.percentile(x, 95),
                       'SPDLIMIT': 'max',
                       'LENGTH': 'max',
                       'FREEFLOW': 'mean',
                       'CONFIDENCE': 'mean'})
    df_tmc = df_tmc.groupby('TMC', as_index=False).agg(tmc_operations)
    return df_tmc


def rename_columns(time_period, df_grouped):
    """Renames default columns for human readability, per AM/PM time period.
    """
    df_final = df_grouped.rename(columns={
        'MEAN': time_period + '_SPEED',
        'MEAN_5': time_period + '_MEAN_5TH_PCTILE',
        'MEAN_95': time_period + '_MEAN_95TH_PCTILE',
        'CONFIDENCE': time_period + '_CONFIDENCE',
        'RELIABILITY': time_period + '_RELIABILITY',
        'CONGESTION': time_period + '_CONGESTION'
    })
    return df_final


def reliability(df_rel):
    """Calculates realiability of speed measures.
    """
    df_rel['RELIABILITY'] = df_rel['MEAN_5'] / df_rel['MEAN']
    return df_rel


def congestion(df_congest):
    """Calculates congestion.
    """
    df_congest['CONGESTION'] = df_congest['MEAN'] / df_congest['SPDLIMIT']
    return df_congest


def main(input):
    """Main script to calculate HERE speed averages."""
    startTime = dt.datetime.now()
    pd.set_option('display.max_rows', None)
    drive_path = 'G:/HERE/data_no_gap_fill/'
    # drive_path = 'G:/HERE/data_gap_filled/'

    df = pd.DataFrame()  # Create empty dataframe
    epochs = []

    if input == 'AM':
        epochs = [30, 31, 32, 33]
    if input == 'PM':
        epochs = [68, 69, 70, 71]

    for csv_file in os.listdir(drive_path):
        df_temp = pd.read_csv(
                    os.path.join(
                        os.path.dirname(__file__), drive_path + csv_file))

        df_temp = df_temp[df_temp['EPOCH-15MIN'].isin(epochs)]

        df = pd.concat([df, df_temp])
        print('Adding {} to dataset.'.format(csv_file))

    # Apply calculation functions
    df['MEAN_95'] = df['MEAN']
    df['MEAN_5'] = df['MEAN']
    df = group_TMC(df)
    df = congestion(df)
    df = reliability(df)
    df = rename_columns(sys.argv[1], df)

    endTime = dt.datetime.now()
    df.to_csv(input + '_speeds.csv')

    print("Script finished in {0}.".format(endTime - startTime))
    print(df.shape)


if __name__ == '__main__':
    main(sys.argv[1])
