import asyncio
import os
import time

import aiohttp as aiohttp
import requests

from config.config_university_project import ConfigUniversityProject
from helper.agent_helper import get_user_random_agent
from helper.array_helper import get_sublists
from helper.async_helper import multi_task_merge_results
from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple
from helper.multithread_helper import multithread_helper
from helper.reader_helper import store_jsons_perline_in_file

logger = LoggerSimple(name=__name__).logger


def get_info(sbd):
    url_api = 'https://diemthi.tuoitre.vn/search-thpt-score'
    body = {"data": sbd, "code": ""}
    data_exam = None

    try:
        response = requests.post(url=url_api, data=body)
        # logger.info(f'{sbd} - {response.text}')
        if response.status_code == 200:
            data = response.json()
            if data.get('data') and len(data.get('data')) > 0 and data.get('data')[0].get('_source'):
                # result =
                # logger.info(response.text)
                data_exam = data.get('data')[0].get('_source')
                # logger.info(f"{sbd} - {data_exam.get('score')}")
    except Exception as e:
        log_error(e)
        logger.error(f'ERROR: sbd={sbd}')

    return data_exam


async def get_info_async(sbd):
    # url_api = 'https://diemthi.tuoitre.vn/search-lop10-score'
    url_api = 'https://diemthi.tuoitre.vn/search-thpt-score'
    body = {"data": sbd, "code": ""}
    data_exam = None
    header = {
        'user-agent': get_user_random_agent(),
        'cookie': 'G_ENABLED_IDPS=google; fpsend=149436; __zi=3000.SSZzejyD3CiaW_sbrKeErsE1gRkRH1QKFvEZf8a6188lrRBZnmC5ncNTjkp33K_1Ojp-xCSEJyPatFlg.1',
    }
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            async with session.post(url=url_api, headers=header, data=body, timeout=30) as res:
                if res.status == 200:
                    response = await res.text()
                    # logger.info(f'{sbd} - {response}')
                    data = await res.json()
                    # logger.info(f'{data.get("data")}')
                    if data.get('data') and len(data.get('data')) > 0 and data.get('data')[0].get('_source'):
                        # result =
                        # logger.info(response.text)
                        data_exam = data.get('data')[0].get('_source')
                        logger.info(f"{sbd} - {data_exam.get('score')}")
    except Exception as e:
        log_error(e)
        logger.error(f'ERROR: sbd={sbd}')

    #     response = requests.post(url=url_api, data=body)
    #     if response.status_code == 200:
    #         # logger.info(f'{sbd} - {response.text}')
    #         data = response.json()
    #         if data.get('data') and len(data.get('data')) > 0 and data.get('data')[0].get('_source'):
    #             # result =
    #             # logger.info(response.text)
    #             data_exam = data.get('data')[0].get('_source')
    #             logger.info(f"{sbd} - {data_exam.get('score')}")
    # except Exception as e:
    #     log_error(e)
    #     logger.error(f'ERROR: sbd={sbd}')

    return data_exam


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
    while should_find:
        if ((min - max) ** 2) == 1:
            # logger.info(f'find end sbd = {mid}')
            break
        mid = int((max - min) / 2) + min
        sbd = build_sbd(provide_id=provide_id, post_sbd=mid)
        logger.info(f'estimate sbd: {sbd}')
        result = get_info(sbd)
        if result is None:
            max = mid
            continue
        else:
            min = mid
            continue
        # max = int(max / 2)

        # info_obj = get_info(sbd)
    # logger.info(f'max = {max} min = {min}')
    return mid


async def job_crawler():
    # lst_provide = ['{0:02}'.format(num) for num in range(1, 65)]
    lst_provide = ['{0:02}'.format(num) for num in range(1, 65)]
    for provide_id in lst_provide:
        try:
            start_at = time.time()
            logger.info(f'prepare crawl provide: {provide_id}')

            # provide_id = 64
            batch_sbd = 5000

            max_sbd = get_min_max_by_code(provide_id)
            logger.info(f'max_sbd: {provide_id} - {max_sbd}')
            # max_sbd = 5743
            lst_sbd = []
            for pos in range(1, max_sbd):
                sbd = build_sbd(provide_id=provide_id, post_sbd=pos)
                lst_sbd.append(sbd)

            lst_task = []
            file_diemthi_path = ConfigUniversityProject().file_diemthi_2021_path(provide_id=provide_id)
            for idx, _sbd in enumerate(lst_sbd):
                # if os.path.exists(file_diemthi_path):
                #     logger.info(f'skip: {file_diemthi_path}')
                #     continue
                # lst_obj_sbd = multithread_helper(items=sub_lst_sbd, method=get_info, timeout_concurrent_by_second=36000,
                #                                  max_workers=50, debug=False)

                lst_task.append(get_info_async(sbd=_sbd))
            lst_obj_sbd = await multi_task_merge_results(lst_task=lst_task, ignore_none=True,
                                                         concurrency=50)
            store_jsons_perline_in_file(jsons_obj=lst_obj_sbd, file_output_path=file_diemthi_path)
            logger.info(
                f'crawled provide_id={provide_id} students={len(lst_obj_sbd)} in {(time.time() - start_at) * 1000} ms -> {file_diemthi_path}')
        except Exception as e:
            logger.error(e)

    # get_info(sbd='02055358')


if __name__ == '__main__':
    # logger.info(get_info(sbd='01000016'))
    # job_crawler()
    asyncio.run(
        # get_info_async(sbd='01000016')
        job_crawler()
    )
    # logger.info(get_min_max_by_code(provide_id='01'))
