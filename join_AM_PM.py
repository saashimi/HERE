import os
import pandas as pd


df_am = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__), 'AM_speeds.csv'))

df_pm = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__), 'PM_speeds.csv'),
        usecols=['TMC', 'PM_MEAN_SPEED', 'PM_SPEED_5TH_PCTILE',
                 'PM_SPEED_95TH_PCTILE', 'PM_CONFIDENCE', 'PM_RELIABILITY', 
                 'PM_CONGESTION'])

df = pd.merge(df_am, df_pm, on='TMC', how='left')

df.to_csv('HERE_AM_PM.csv')
