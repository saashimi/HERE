"""
Script to calculate average HERE speeds.
Calculates mean and 5th and 95th percentile speeds per AM and PM peak.
Script by Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov
Useage:
    >>> python HERE_parser.py <AM or PM>
This script is intended to be used before join_AM_pm.py, which will join the
AM and PM speed calculations into a single CSV file.
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
    tmc_operations = ({'LENGTH': 'max',
                       'SPDLIMIT': 'max',
                       'FREEFLOW': 'mean',
                       'MEAN': 'mean',
                       'MEAN_5': lambda x: np.percentile(x, 5),
                       'MEAN_95': lambda x: np.percentile(x, 95),
                       'CONFIDENCE': 'mean'})
    df_tmc = df_tmc.groupby('TMC', as_index=False).agg(tmc_operations)
    return df_tmc


def rename_columns(time_period, df_grouped):
    """Renames default columns for human readability, per AM/PM time period.
    Args: time_period, which is user input `AM` or `PM`.
          df_grouped, a pandas dataframe
    Returns: df_final, a pandas dataframe with renamed columns.
    """
    df_final = df_grouped.rename(columns={
        'MEAN': time_period + '_MEAN_SPEED',
        'MEAN_5': time_period + '_SPEED_5TH_PCTILE',
        'MEAN_95': time_period + '_SPEED_95TH_PCTILE',
        'CONFIDENCE': time_period + '_CONFIDENCE',
        'RELIABILITY': time_period + '_RELIABILITY',
        'CONGESTION': time_period + '_CONGESTION'
    })
    return df_final


def reliability(df_rel):
    """Calculates realiability of speed measures.
    Args: df_rel, a pandas dataframe.
    Returns: df_rel with new column `RELIABILITY`.
    """
    df_rel['RELIABILITY'] = (df_rel['MEAN_5'] / df_rel['MEAN']).round(3)
    return df_rel


def revise_speed_limits(df_lmt):
    """Update existing dataset with revised speed limits per TMC. Searches for
    NaN values in `SPDLIMIT` against `CHECK_SPDLIMIT` column in revised dataset.
    """
    file_path = 'G:/corridors/swcorr/ris/HERE_data/Metro_revised_091918/'
    df_new_lmt = pd.read_csv(os.path.join(file_path, 'tmc_speedlimit.csv'))
    
    tmc_operations = ({'CHECK_SPDLIMIT': 'max'})
    df_new_lmt = df_new_lmt.groupby('TMC', as_index=False).agg(tmc_operations)
    
    df_revised = pd.merge(df_lmt, df_new_lmt, on='TMC', how='left')
    df_revised['REV_SPD'] = np.where(
                                     pd.isnull(df_revised['SPDLIMIT']), 
                                     df_revised['CHECK_SPDLIMIT'], 
                                     df_revised['SPDLIMIT'])
    
    return df_revised


def congestion(df_congest):
    """Calculates congestion.
    Args: df_congest, a pandas dataframe.
    Returns: df_congest with new column `CONGESTION`.
    """
    df_congest['CONGESTION'] = (df_congest['MEAN'] /
                                df_congest['SPDLIMIT']).round(3)
    return df_congest


def main(input):
    """Main script to calculate HERE speed averages."""
    startTime = dt.datetime.now()
    pd.set_option('display.max_rows', None)
    drive_path = 'G:/corridors/swcorr/ris/HERE_data/data_no_gap_fill/'
    # drive_path = 'G:/corridors/swcorr/ris/HERE_data/data_gap_filled/'

    df = pd.DataFrame()  # Create empty dataframe
    epochs = []

    if input == 'AM':
        epochs = [30, 31, 32, 33]
    if input == 'PM':
        epochs = [68, 69, 70, 71]

    # Revised script for 2017 data only.
    files_2017 = ['HERE_DA_15674.csv',
                  'HERE_DA_15675.csv',
                  'HERE_DA_15680.csv',
                  'HERE_DA_15681.csv',
                  'HERE_DA_15686.csv',
                  'HERE_DA_15687.csv']
    
    # Concatenate all files into single dataframe.
    for csv_file in os.listdir(drive_path):
        if csv_file in files_2017:
            df_temp = pd.read_csv(
                        os.path.join(
                            os.path.dirname(__file__), drive_path + csv_file))
            df_temp = df_temp[df_temp['EPOCH-15MIN'].isin(epochs)]
            df = pd.concat([df, df_temp])
            print('Adding {} to dataset.'.format(csv_file))

    # Apply calculation functions
    df['MEAN_95'] = df['MEAN']
    df['MEAN_5'] = df['MEAN']
    df = revise_speed_limits(df)
    df = group_TMC(df)
    df = congestion(df)
    df = reliability(df)
    df = rename_columns(sys.argv[1], df)

    endTime = dt.datetime.now()
    df.to_csv(input + '_speeds_091918.csv', index=False)
    print('Script finished in {0}.'.format(endTime - startTime))
    print('Final CSV contains {0} rows.'.format(df.shape[0]))


if __name__ == '__main__':
    main(sys.argv[1])
