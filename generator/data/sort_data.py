import pandas as pd

DATA_PATH_TRAIN = 'train.csv'

DATA_PATH_TEST = 'train.csv'


train_df = pd.read_csv('train.csv')
test_df = pd.read_csv('test.csv')

df = pd.concat([train_df, test_df])

df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])

# Sort the DataFrame by the 'timestamp' column
df_sorted = df.sort_values(by='pickup_datetime')

df_sorted.to_csv('data_sorted.csv', index=False)