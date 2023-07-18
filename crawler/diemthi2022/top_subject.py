from helper.array_helper import sort_by_value
from helper.reader_helper import get_files_absolute_in_folder, load_jsonl_from_gz, store_jsons_perline_in_file


def calculate_top_group_a(folder_diemthi_2021_path):
    lst_exam = []
    for file_gz_path in get_files_absolute_in_folder(folder_path=folder_diemthi_2021_path):
        for obj in load_jsonl_from_gz(file_gz_path=file_gz_path):
            if obj.get('Toan') is not None and obj.get('Li') is not None and obj.get('Hoa') is not None:
                total_subject_a00 = float(obj.get('Toan')) + float(obj.get('Li')) + float(obj.get('Hoa'))
                sbd = obj.get('sbd')
                if total_subject_a00 >= 27:
                    lst_exam.append({
                        'sbd': sbd,
                        'a00': total_subject_a00
                    })
    lst_exam = sort_by_value(array_dict=lst_exam, key='a00')
    store_jsons_perline_in_file(jsons_obj=lst_exam,
                                file_output_path='/bee_university/crawler/common/2021/top_group_a.json.gz')
    print(f'{len(lst_exam)} exams')


if __name__ == '__main__':
    folder_transform = '/bee_university/crawler/common/diemthi_2021_transform'
    calculate_top_group_a(folder_diemthi_2021_path=folder_transform)
