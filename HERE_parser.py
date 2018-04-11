"""
Script to calculate average HERE speeds.
Script by Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov
"""

import os
import pandas as pd
import numpy as np
import datetime as dt

# Conversion factors
KPM_TO_MPH = 0.621371
METERS_TO_MILES = 0.000621371


def speed_to_travel_time(length, speed):
    """
    Converts speeds to travel time.
    Args: Segment length, speed
    """
    travel_time = (length * 60) / speed
    return travel_time


def group_TMC(df_tmc):
    """Groups by TMC and aggregates average speeds.
    Args: df_tmc, a pandas dataframe.
    Returns: df_tmc, a pandas dataframe grouped by TMC.
    """
    tmc_operations = ({'MEAN': 'mean',
                       'FREEFLOW': 'mean',
                       'CONFIDENCE': 'mean'})
    df_tmc = df_tmc.groupby('TMC', as_index=False).agg(tmc_operations)
    return df_tmc


def main():
    """Main script to calculate HERE speed averages."""
    startTime = dt.datetime.now()
    pd.set_option('display.max_rows', None)
    drive_path = 'G:/HERE/data_no_gap_fill/'

    df = pd.DataFrame()  # Create empty dataframe

    for csv_file in os.listdir(drive_path):
        df_temp = pd.read_csv(
                    os.path.join(
                        os.path.dirname(__file__), drive_path + csv_file))  
        
        # Filter out for epoch 07:30 - 08:30
        df_temp = df_temp[df_temp['EPOCH-15MIN'].isin([30, 31, 32, 33, 34])] 

        df = pd.concat([df, df_temp])
        print('Adding {} to dataset.'.format(csv_file))

    """
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), drive_path + 'HERE_DA_15674.csv'))
    df = df[df['EPOCH-15MIN'].isin([30, 31, 32, 33, 34])] # Epoch 07:30 - 08:30 
    """    

    df = group_TMC(df)
    endTime = dt.datetime.now()
    
    print("Script finished in {0}.".format(endTime - startTime))
    print(df)
    print(df.shape)


if __name__ == '__main__':
    main()