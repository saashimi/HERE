"""
Script to calculates total congestion hours per link.
Script by Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov
Useage:
    >>> python HERE_parser_all_day.py

"""

import os
import pandas as pd
import numpy as np
import datetime as dt


def rename_columns(df_grouped):
    """Renames default columns for human readability.
    Args: df_grouped, a pandas dataframe
    Returns: df_final, a pandas dataframe with renamed columns.
    """
    df_final = df_grouped.rename(columns={
        'REV_SPD' : 'SPDLIMIT',
        'MEAN': 'MEAN_SPEED',
        'MEAN_5': 'SPEED_5TH_PCTILE',
        'MEAN_95': 'SPEED_95TH_PCTILE',
        'CONFIDENCE': 'CONFIDENCE',
        'RELIABILITY': 'RELIABILITY',
        'CONGESTION': 'CONGESTION'
    })
    return df_final


def group_TMC(df_tmc):
    """Groups by TMC and aggregates average speeds.
    Args: df_tmc, a pandas dataframe.
    Returns: df_tmc, a pandas dataframe grouped by TMC.
    """
    tmc_operations = ({'LENGTH': 'max',
                       'REV_SPD': 'max',
                       'FREEFLOW': 'mean',
                       'HR_35_PCT_SPDLMT': 'sum',
                       'HR_50_PCT_SPDLMT': 'sum',
                       'HR_75_PCT_SPDLMT': 'sum',
                       'MEAN': 'mean',
                       'CONFIDENCE': 'mean'})
    df_tmc = df_tmc.groupby('TMC', as_index=False).agg(tmc_operations)
    return df_tmc


def spd_threshhold(df_spdt):
    """Calculates total hours of congestion based on 35%, 50%, and 75% speed
    limit speeds. Data is given in .25 hr (15-min) increments.
    Args: df_spdt, a pandas dataframe.
    Returns: df_spdt with new columns:
        `HR_35_PCT_SPDLMT`
        `HR_50_PCT_SPDLMT`
        `HR_75_PCT_SPDLMT`
    """
    pct_spdlmt_35 = .35 * df_spdt['SPDLIMIT'].round(3)
    pct_spdlmt_50 = .5 * df_spdt['SPDLIMIT'].round(3)
    pct_spdlmt_75 = .75 * df_spdt['SPDLIMIT'].round(3)
    # Condition tallies data in .25-hr increments.
    df_spdt['HR_35_PCT_SPDLMT'] = np.where(
        df_spdt['MEAN'] < pct_spdlmt_35, .25, 0)
    df_spdt['HR_50_PCT_SPDLMT'] = np.where(
        df_spdt['MEAN'] < pct_spdlmt_50, .25, 0)
    df_spdt['HR_75_PCT_SPDLMT'] = np.where(
        df_spdt['MEAN'] < pct_spdlmt_75, .25, 0)
    return df_spdt


def revise_speed_limits(df_lmt):
    """Update existing dataset with revised speed limits per TMC. Searches for
    NaN values in `SPDLIMIT` against `CHECK_SPDLIMIT` column in revised dataset.
    """
    file_path = 'G:/corridors/swcorr/ris/HERE_data/Metro_revised_091918/'
    df_new_lmt = pd.read_csv(os.path.join(file_path, 'tmc_speedlimit.csv'))
    df_revised = pd.merge(df_lmt, df_new_lmt, on='TMC', how='left')
    df_revised['REV_SPD'] = np.where(pd.isnull(df_revised['SPDLIMIT']), df_revised['CHECK_SPDLIMIT'], df_revised['SPDLIMIT'])
    
    return df_revised


def main():
    """Main script to calculate time spent in congestion."""
    startTime = dt.datetime.now()
    pd.set_option('display.max_rows', None)
    drive_path = 'G:/corridors/swcorr/ris/HERE_data/data_no_gap_fill/'
    # drive_path = 'G:/corridors/swcorr/ris/HERE_data/data_gap_filled/'

    df = pd.DataFrame()  # Create empty dataframe

    # Parse for 2017 Data only
    # Concatenate all files into single dataframe.
    files_2017 = ['HERE_DA_15674.csv',
                  'HERE_DA_15675.csv',
                  'HERE_DA_15680.csv',
                  'HERE_DA_15681.csv',
                  'HERE_DA_15686.csv',
                  'HERE_DA_15687.csv']
    for csv_file in os.listdir(drive_path):
        if csv_file in files_2017:
            df_temp = pd.read_csv(
                        os.path.join(
                            os.path.dirname(__file__), drive_path + csv_file))
            df = pd.concat([df, df_temp])
            print('Adding {} to dataset.'.format(csv_file))

    # Apply calculation functions
    # print(df.loc[df['TMC'] == '114N04444'])
    df = revise_speed_limits(df)
    df = spd_threshhold(df)
    df = group_TMC(df)
    df = rename_columns(df)

    endTime = dt.datetime.now()
    df.to_csv('all_day_congestion_091918.csv', index=False)
    print('Script finished in {0}.'.format(endTime - startTime))
    print('Final CSV contains {0} rows.'.format(df.shape[0]))


if __name__ == '__main__':
    main()
