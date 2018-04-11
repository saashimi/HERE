"""
Script to calculate average HERE speeds.
Script by Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov
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
                       'MEAN_5': lambda x: np.percentile(x, .05),
                       'MEAN_95': lambda x: np.percentile(x, .95),
                       'FREEFLOW': 'mean',
                       'CONFIDENCE': 'mean'})
    df_tmc = df_tmc.groupby('TMC', as_index=False).agg(tmc_operations)
    return df_tmc


def rename_columns(time_period, df_grouped):
    df_final = df_grouped.rename(columns={
        'MEAN': time_period + '_MEAN',
        'MEAN_5': time_period + '_MEAN_5',
        'MEAN_95': time_period + '_MEAN_95',
        'FREEFLOW': time_period + '_FREEFLOW',
        'CONFIDENCE': time_period + '_CONFIDENCE'
    })
    return df_final


def main(input):
    """Main script to calculate HERE speed averages."""
    startTime = dt.datetime.now()
    pd.set_option('display.max_rows', None)
    drive_path = 'G:/HERE/data_no_gap_fill/'

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

        # Filter out for epoch 07:30 - 08:30
        df_temp = df_temp[df_temp['EPOCH-15MIN'].isin(epochs)]

        df = pd.concat([df, df_temp])
        print('Adding {} to dataset.'.format(csv_file))

    df['MEAN_95'] = df['MEAN']
    df['MEAN_5'] = df['MEAN']
    df = group_TMC(df)
    df = rename_columns(sys.argv[1], df)
    endTime = dt.datetime.now()
    df.to_csv(input + '_speeds.csv')

    print("Script finished in {0}.".format(endTime - startTime))
    print(df.shape)


if __name__ == '__main__':
    main(sys.argv[1])
