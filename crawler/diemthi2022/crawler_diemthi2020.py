#   Copyright (c) 2019 BeeCost Team <beecost.com@gmail.com>. All Rights Reserved
#   BeeCost Project can not be copied and/or distributed without the express permission of @tuantmtb
import os

import requests
from bs4 import BeautifulSoup

from config.config_university_project import ConfigUniversityProject
from helper.array_helper import get_sublists
from helper.logger_helper import LoggerSimple
from helper.multithread_helper import multithread_helper
from helper.reader_helper import store_jsons_perline_in_file

logger = LoggerSimple(name=__name__).logger


def get_url_check_sbd(sbd):
    # return f'https://diemthi.tuoitre.vn/kythi2020.html?FiledValue={sbd}&MaTruong={sbd[:2]}'
    return f'https://diemthi.tuoitre.vn/kythi2020.html?FiledValue={sbd}&MaTruong=diemthi'


def get_info(sbd):
    info_obj = None
    try:
        url_diemthi = get_url_check_sbd(sbd=str(sbd))
        response = requests.get(url_diemthi)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # logger.info(soup)
            if soup.select_one('#ctl01_SoBD') is not None:
                _sbd = soup.select_one('#ctl01_SoBD').get_text().strip()
                points = []
                info_obj = info_obj = {
                    'sbd': _sbd,
                }
                for idx, td in enumerate(soup.select('.color-red')):
                    if td.get_text().strip() != '-':
                        point = None
                        subject = None
                        if idx == 9:
                            point = td.get_text().strip()
                        else:
                            point = float(td.get_text().strip())
                        # logger.info(point)

                        if idx == 0:
                            subject = 'Toan'
                        elif idx == 1:
                            subject = 'Van'
                        elif idx == 2:
                            subject = 'Li'
                        elif idx == 3:
                            subject = 'Hoa'
                        elif idx == 4:
                            subject = 'Sinh'
                        elif idx == 5:
                            subject = 'Su'
                        elif idx == 6:
                            subject = 'Dia'
                        elif idx == 7:
                            subject = 'GDCD'
                        elif idx == 8:
                            subject = 'Ngoai_ngu'
                        elif idx == 9:
                            subject = 'Ma_mon_ngoai_ngu'

                        info_obj.update({
                            subject: point
                        })

                # logger.info(f'found {sbd}')
                logger.info(info_obj)
    except Exception as e:
        logger.error(e)
    return info_obj


def build_sbd(provide_id, post_sbd):
    prefix = ''.join(['0' for i in range(6 - len(str(post_sbd)))])
    # logger.info(prefix)
    return f'{provide_id}{prefix}{post_sbd}'


def get_min_max_by_code(provide_id='64'):
    min = 1
    max = 999999

    sbd = max
    should_find = True
    mid = int((max - min) / 2) + min
    while (should_find):
        if ((min - max) ** 2) == 1:
            # logger.info(f'find end sbd = {mid}')
            break
        mid = int((max - min) / 2) + min
        sbd = build_sbd(provide_id=provide_id, post_sbd=mid)
        if get_info(sbd) is None:
            max = mid
            continue
        else:
            min = mid
            continue
        # max = int(max / 2)

        # info_obj = get_info(sbd)
    # logger.info(f'max = {max} min = {min}')
    return mid


if __name__ == '__main__':
    # url_diemthi = 'https://dantri.com.vn/tra-cuu-diem-thi-tot-nghiep-thpt-quoc-gia-2019.htm'
    # sbd = 64005452

    # sbd = 64000001
    # for sbd in reversed(range(64000000, 64003999)):
    #     info_obj = get_info(sbd=sbd)
    #     logger.info(f'{sbd} - {info_obj}')

    lst_provide = ['{0:02}'.format(num) for num in range(1, 65)]
    for provide_id in lst_provide:
        try:
            logger.info(f'prepare crawl provide: {provide_id}')

            # provide_id = 64
            batch_sbd = 5000

            max_sbd = get_min_max_by_code(provide_id)
            # logger.info(max_sbd)
            # max_sbd = 5743
            lst_sbd = []
            for pos in range(1, max_sbd):
                sbd = build_sbd(provide_id=provide_id, post_sbd=pos)
                lst_sbd.append(sbd)

            for idx, sub_lst_sbd in enumerate(get_sublists(lst_sbd, int(len(lst_sbd) / 5000) + 1)):
                file_diemthi_path = ConfigUniversityProject().file_diemthi_2020_path(provide_id=provide_id, part=idx)
                if os.path.exists(file_diemthi_path):
                    logger.info(f'skip: {file_diemthi_path}')
                    continue
                obj_sbd = multithread_helper(items=sub_lst_sbd, method=get_info, timeout_concurrent_by_second=36000,
                                             max_workers=50, debug=False)
                store_jsons_perline_in_file(jsons_obj=obj_sbd, file_output_path=file_diemthi_path)
                logger.info(f'write: {file_diemthi_path}')
        except Exception as e:
            logger.error(e)
    logger.info('done')
