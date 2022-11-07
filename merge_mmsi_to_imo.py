import os
import pandas as pd


root_path = 'E:/AISData/' # AIS folder

def merge_mmsi_to_imo(year_month='201901'):
    """
    根据IMO编号，将属于同一条船的不同MMSI编号对应的动态信息文件进行合并，并以IMO编号为文件名进行存储
    以2019年为例，原始数据结构树如下所示：
    root_path/AIS2019/
    |————SRC/
    |     |——201901/
    |          |——316013356.csv
    |          |——701005938.csv
    |          ...
    |     |——201902/
    |          ...
    |     |——201903/
    |          ...
    |————STATIC/
    |     |——ihs_2015.csv
    |     |——ihs_2016.csv
    |     |——ihs_2017.csv
    |     |——ihs_2018.csv
    |     |——ihs_2019.csv
    """
    year = year_month[:4]

    df_ihs = pd.read_csv(root_path + 'AIS' + year + '/STATIC/ihs_' + year + '.csv', encoding='gbk') # AIS STAITC DATA
    imo_set = df_ihs['imo'].drop_duplicates().values

    data_path = root_path + 'AIS' + year + '/SRC/'
    save_path = root_path + 'AIS' + year + '/IMO/'

    month_path = os.path.join(data_path, year_month)
    save_month = os.path.join(save_path, year_month)

    if not os.path.exists(save_month):
        os.makedirs(save_month)

    for id, imo in enumerate(imo_set):
        df_info = df_ihs[df_ihs.imo == imo]
        dest_file = os.path.join(save_month, str(imo) + '.csv') # save the result as a file
        if os.path.exists(dest_file): # if the result file exists, skip
            continue
            
        if df_info.shape[0] == 1: 
            mmsi = df_info.mmsi.values[0]
            src_file = os.path.join(month_path, str(mmsi) + '.csv')
            df_temp = pd.read_csv(src_file, encoding='gbk')
            df_temp['IMO'] = imo
            df_temp.to_csv(dest_file, index=False)
        elif df_info.shape[0] > 1:
            ls_dfs = []
            for idx, row in df_info.iterrows():
                mmsi = row['mmsi']
                src_file = os.path.join(month_path, str(mmsi) + '.csv')

                df_temp = pd.read_csv(src_file, encoding='gbk')
                df_temp['IMO'] = imo
                ls_dfs.append(df_temp)

            df_res = pd.concat(ls_dfs)
            df_res['DateTime'] = pd.to_datetime(df_res['DateTime'])
            df_res = df_res.sort_values(by='DateTime').reset_index(drop=True)
            df_res.to_csv(dest_file, index=False)
            

if __name__ == '__main__':
    print('in file_process.py')
    pass
