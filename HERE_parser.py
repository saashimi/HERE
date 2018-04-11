"""
Script to calculate average HERE speeds.
Script by Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov
"""

import os
import pandas as pd
import numpy as np
import datetime as dt


def main():
    """Main script to calculate HERE speed averages."""
    startTime = dt.datetime.now()
    pd.set_option('display.max_rows', None)
    drive_path = 'data/data_no_gap_fill/'

    df = pd.DataFrame()  # Create empty dataframe

    
    for csv_file in os.listdir(drive_path):
        df_temp = pd.read_csv(
                    os.path.join(
                        os.path.dirname(__file__), drive_path + csv_file))
        
        df_temp = df_temp[df_temp['EPOCH-15MIN'].isin([30, 31, 32, 33, 34])] 
        # Epoch 07:30 - 08:30 
        df = pd.concat([df, df_temp])
        print('Adding {} to dataset.'.format(csv_file))

    
    """
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), drive_path + 'HERE_DA_15674.csv'))
    df = df[df['EPOCH-15MIN'].isin([30, 31, 32, 33, 34])] # Epoch 07:30 - 08:30 
    """    

    endTime = dt.datetime.now()
    
    print("Script finished in {0}.".format(endTime - startTime))
    print(df.shape)

if __name__ == '__main__':
    main()