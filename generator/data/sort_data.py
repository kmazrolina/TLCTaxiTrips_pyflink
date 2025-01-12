import pandas as pd

train_df = pd.read_csv('train.csv')

train_df['pickup_datetime'] = pd.to_datetime(train_df['pickup_datetime'])
train_df['trip_duration'] = train_df['trip_duration'].round().astype(int) 
# Sort the DataFrame by the 'timestamp' column
df_sorted = train_df.sort_values(by='pickup_datetime')

df_sorted.to_csv('data_sorted.csv', index=False)