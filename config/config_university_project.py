class ConfigUniversityProject:

    def __init__(self, *args, **kwargs):
        self.folder_data_base = kwargs.get('folder_data_base', '/bee_university')

    @property
    def folder_output_path(self):
        return self.folder_data_base + '/crawler/common'

    @property
    def file_university_path(self):
        return self.folder_output_path + '/university.gz'

    @property
    def file_university_diemchuan_path(self):
        return self.folder_output_path + '/university_diemchuan.gz'

    def file_diemthi_2020_path(self, provide_id, part):
        return f'{self.folder_output_path}/diemthi_2020/provide_{provide_id}_{part}.gz'

    def file_diemthi_2021_path(self, provide_id, part=None):
        if part is None:
            return f'{self.folder_output_path}/diemthi_2021/provide_{provide_id}.gz'
        return f'{self.folder_output_path}/diemthi_2021/provide_{provide_id}_{part}.gz'

    @property
    def file_major_path(self):
        return self.folder_output_path + '/major.gz'
