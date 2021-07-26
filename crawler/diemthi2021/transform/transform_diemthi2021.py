from helper.logger_helper import LoggerSimple

from helper.reader_helper import store_gz, get_files_absolute_in_folder, load_jsonl_from_gz, store_jsons_perline_in_file

logger = LoggerSimple(name=__name__).logger


def job_transform(folder_diemthi_2021_path, folder_diemthi_2021_transform_path):
    for file_gz_path in get_files_absolute_in_folder(folder_path=folder_diemthi_2021_path):
        file_name = file_gz_path.split('/')[-1]
        file_diemthi_transform_path = f'{folder_diemthi_2021_transform_path}/{file_name}'
        lst_exam = []
        for obj in load_jsonl_from_gz(file_gz_path=file_gz_path):
            # logger.info(obj)
            sbd = obj.get('studentCode')
            if sbd is None:
                continue
            exam_data = {
                'sbd': sbd
            }
            if obj.get('TOAN') is not None and len(obj.get('TOAN')) > 0:
                exam_data.update({'Toan': obj.get('TOAN')})
            if obj.get('VAN') is not None and len(obj.get('VAN')) > 0:
                exam_data.update({'Van': obj.get('VAN')})
            if obj.get('LY') is not None and len(obj.get('LY')) > 0:
                exam_data.update({'Li': obj.get('LY')})
            if obj.get('HOA') is not None and len(obj.get('HOA')) > 0:
                exam_data.update({'Hoa': obj.get('HOA')})
            if obj.get('SINH') is not None and len(obj.get('SINH')) > 0:
                exam_data.update({'Sinh': obj.get('SINH')})
            if obj.get('Su') is not None and len(obj.get('Su')) > 0:
                exam_data.update({'SU': obj.get('Su')})
            if obj.get('Dia') is not None and len(obj.get('Dia')) > 0:
                exam_data.update({'DIA': obj.get('Dia')})
            if obj.get('GDCD') is not None and len(obj.get('GDCD')) > 0:
                exam_data.update({'GDCD': obj.get('GDCD')})
            if obj.get('NGOAINGU') is not None and len(obj.get('NGOAINGU')) > 0:
                exam_data.update({'Ngoai_ngu': obj.get('NGOAINGU')})
            if obj.get('CODE_NGOAINGU') is not None and len(obj.get('CODE_NGOAINGU')) > 0:
                exam_data.update({'Ma_mon_ngoai_ngu': obj.get('CODE_NGOAINGU')})
            lst_exam.append(exam_data)
            # logger.info(exam_data)
        if lst_exam is not None and len(lst_exam) > 0:
            store_jsons_perline_in_file(jsons_obj=lst_exam, file_output_path=file_diemthi_transform_path)
        logger.info(f'transformed {len(lst_exam)} students -> {file_diemthi_transform_path}')


if __name__ == '__main__':
    folder_diemthi_2021_path = '/bee_university/crawler/common/diemthi_2021'
    folder_diemthi_2021_transform_path = '/bee_university/crawler/common/diemthi_2021_transform'
    job_transform(
        folder_diemthi_2021_path=folder_diemthi_2021_path,
        folder_diemthi_2021_transform_path=folder_diemthi_2021_transform_path)
