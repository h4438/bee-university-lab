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
                'id': sbd
            }
            exam_data.update({'year': 2021})
            if obj.get('TOAN') is not None and len(obj.get('TOAN')) > 0:
                exam_data.update({'mathematics_score': obj.get('TOAN')})
            if obj.get('VAN') is not None and len(obj.get('VAN')) > 0:
                exam_data.update({'literature_score': obj.get('VAN')})
            if obj.get('LY') is not None and len(obj.get('LY')) > 0:
                exam_data.update({'physics_score': obj.get('LY')})
            if obj.get('HOA') is not None and len(obj.get('HOA')) > 0:
                exam_data.update({'chemistry_score': obj.get('HOA')})
            if obj.get('SINH') is not None and len(obj.get('SINH')) > 0:
                exam_data.update({'biology_score': obj.get('SINH')})
            if obj.get('SU') is not None and len(obj.get('SU')) > 0:
                exam_data.update({'history_score': obj.get('SU')})
            if obj.get('DIA') is not None and len(obj.get('DIA')) > 0:
                exam_data.update({'geography_score': obj.get('DIA')})
            if obj.get('GDCD') is not None and len(obj.get('GDCD')) > 0:
                exam_data.update({'civic_education_score': obj.get('GDCD')})
            if obj.get('NGOAINGU') is not None and len(obj.get('NGOAINGU')) > 0:
                exam_data.update({'foreign_language_score': obj.get('NGOAINGU')})
            if obj.get('CODE_NGOAINGU') is not None and len(obj.get('CODE_NGOAINGU')) > 0:
                code_language = obj.get('CODE_NGOAINGU')

                exam_data.update({'foreign_language_type': code_language})
                if code_language == 'N1':
                    exam_data.update({'english_score': obj.get('NGOAINGU')})
                elif code_language == 'N2':
                    exam_data.update({'russian_score': obj.get('NGOAINGU')})
                elif code_language == 'N3':
                    exam_data.update({'french_score': obj.get('NGOAINGU')})
                elif code_language == 'N4':
                    exam_data.update({'chinese_score': obj.get('NGOAINGU')})
                elif code_language == 'N5':
                    exam_data.update({'german_score': obj.get('NGOAINGU')})
                elif code_language == 'N6':
                    exam_data.update({'japanese_score': obj.get('NGOAINGU')})
                elif code_language == 'N7':
                    exam_data.update({'korean_score': obj.get('NGOAINGU')})
                else:
                    logger.info(f'ERROR: {code_language} - {sbd}')

            lst_exam.append(exam_data)
            # logger.info(exam_data)
        if lst_exam is not None and len(lst_exam) > 0:
            store_jsons_perline_in_file(jsons_obj=lst_exam, file_output_path=file_diemthi_transform_path)
        logger.info(f'transformed {len(lst_exam)} students -> {file_diemthi_transform_path}')


if __name__ == '__main__':
    folder_diemthi_2021_path = '/bee_university/crawler/common/diemthi_2021'
    folder_diemthi_2021_transform_path = '/bee_university/crawler/common/diemthi_2021_transform'
    # folder_diemthi_2021_transform_path = '/bee_university/crawler/common/diemthi_2021'
    job_transform(
        folder_diemthi_2021_path=folder_diemthi_2021_path,
        folder_diemthi_2021_transform_path=folder_diemthi_2021_transform_path)
