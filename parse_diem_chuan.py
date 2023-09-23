import pandas as pd
import os

config = {"parsed_folder": "../diem_chuan"}


def split_json_df(file_name:str):
    df = pd.read_json(file_name, lines=True)
    diem_key = "diemchuan_datas"
    truong_key = "university_meta"
    target_folder = config['parsed_folder']
    for idx, row in df.iterrows():
        uni_name = row[truong_key]["university_name"]
        print(f"=> {idx}_{uni_name}")
        uni_df = pd.DataFrame(row[diem_key])
        uni_df['uni_name'] = uni_name
        print(uni_df.shape)
        print(uni_df.columns)
        path = os.path.join(target_folder, uni_name.replace(" ", "_")+".json")
        uni_df.to_json(path, lines=True, orient="records", force_ascii=False)

if __name__ == "__main__":
    file = "./university_diemchuan.json"
    split_json_df(file)
