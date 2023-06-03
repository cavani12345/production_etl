import pandas as pd

class XetraTransform():
    def __init__(self,src_bucket,target_bucket):
        self.src_bucket = src_bucket
        self.target_bucket = target_bucket


    def extract(self):
        keys = self.src_bucket.list_files_in_prefix()
        df_all = pd.concat([self.src_bucket.read_csv_to_df(key) for key in keys])
        return df_all

    def load(self,dataframe,key='proccesed_data_new_2022-01-31.csv'):
        return self.target_bucket.write_df_to_s3(dataframe,key)

    def full_etl(self):
        dataframe = self.extract()
        self.load(dataframe)
        return True
