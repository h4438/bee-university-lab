#   Copyright (c) 2020 BeeCost Team <beecost.com@gmail.com>. All Rights Reserved
#   BeeCost Project can not be copied and/or distributed without the express permission of @tuantmtb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from helper.logger_helper import LoggerSimple
from helper.reader_helper import get_files_absolute_in_folder
from helper.reader_helper import load_jsonl_from_gz

logger = LoggerSimple(name=__name__).logger


def load_data(diemthi_2020_folder_path='/bee_university/crawler/common/diemthi_2020', number_of_file_load=-1):
    data = []
    for idx, diemthi_2020_file_path in enumerate(get_files_absolute_in_folder(diemthi_2020_folder_path)):
        logger.info(f'loading {diemthi_2020_file_path}')
        data += load_jsonl_from_gz(diemthi_2020_file_path)
        if number_of_file_load != -1:
            if idx > number_of_file_load:
                break
    df = pd.DataFrame(data)
    return df


def read_data(parquet_file):
    df = pd.read_parquet(parquet_file)
    return df


if __name__ == '__main__':
    pd.option_context('display.max_columns', 40)

    diemthi_2020_folder_path = '/bee_university/crawler/common/diemthi_2020'
    diemthi_2020_parquet_file_path = '/bee_university/crawler/common/diemthi_2020_transform/diemthi2020.parquet'
    diemthi_2020_csv_file_path = '/bee_university/crawler/common/diemthi_2020_transform/diemthi2020.csv.gz'
    df = load_data(diemthi_2020_folder_path=diemthi_2020_folder_path)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, diemthi_2020_parquet_file_path)
    df = read_data(parquet_file=diemthi_2020_parquet_file_path)
    df.to_csv(diemthi_2020_csv_file_path, compression='gzip')
    logger.info(df.describe(include='all'))

    # diemthi_2020_file_path = '/bee_university/crawler/common/diemthi_2020/provide_01_0.gz'
    # data = load_jsonl_from_gz(diemthi_2020_file_path)
    # df = pd.DataFrame(data)
    # logger.info(f'{df.describe()}')
