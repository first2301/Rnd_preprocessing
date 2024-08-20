import os
import polars as pl
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiofiles  # 비동기 파일 처리 라이브러리


# class PolarsDataFrame:
#     '''
#     polars dataframe 생성
#     '''
#     def __init__(self):
#         pass

#     async def _extract_data(self, paths, extractor):
#         # 각 파일 경로에 대해 extractor 함수를 비동기적으로 실행
#         return await asyncio.gather(*(extractor(path) for path in paths))

#     async def extract_file_id(self, paths):
#         return await self._extract_data(paths, lambda path: os.path.splitext(os.path.basename(path))[0])

#     async def extract_file_name(self, paths):
#         return await self._extract_data(paths, lambda path: os.path.basename(path))

#     async def extract_folder_name(self, paths):
#         return await self._extract_data(paths, lambda path: os.path.basename(os.path.dirname(path)))

#     async def extract_file_size(self, paths):
#         async def get_size(path):
#             async with aiofiles.open(path, 'rb') as f:
#                 await f.seek(0, os.SEEK_END)  # 파일 끝으로 이동
#                 return f.tell()  # 파일 크기 반환

#         return await self._extract_data(paths, get_size)

#     async def get_polars_dataframe(self, paths):
#         '''polars_dataframe 생성'''
#         # 각 비동기 작업의 결과를 await하고 수집
#         file_ids = await self.extract_file_id(paths)
#         file_names = await self.extract_file_name(paths)
#         folder_names = await self.extract_folder_name(paths)
#         file_sizes = await self.extract_file_size(paths)
        
#         # 결과를 DataFrame으로 변환
#         df = pl.DataFrame({
#             "full_path": paths,
#             "file_id": file_ids,
#             "file_name": file_names,
#             "folder_name": folder_names,
#             "file_size": file_sizes
#         })
#         return df

    # async def get_polars_dataframe(self, paths):
    #     '''polars_dataframe 생성'''
    #     df = pl.DataFrame({
    #         "full_path": paths,
    #         "file_id": await self.extract_file_id(paths),
    #         "file_name": await self.extract_file_name(paths),
    #         "folder_name": await self.extract_folder_name(paths),
    #         "file_size": await self.extract_file_size(paths)
    #     })
    #     return df

class PolarsDataFrame:
    '''
    polars dataframe 생성
    '''
    def __init__(self):
        pass

    def _extract_data(self, paths, extractor):
        with ThreadPoolExecutor() as executor:
            return list(executor.map(extractor, paths))

    def extract_file_id(self, paths):
        return self._extract_data(paths, lambda path: os.path.splitext(os.path.basename(path))[0])

    def extract_file_name(self, paths):
        return self._extract_data(paths, lambda path: os.path.basename(path))

    def extract_folder_name(self, paths):
        return self._extract_data(paths, lambda path: os.path.basename(os.path.dirname(path)))

    def extract_file_size(self, paths):
        return self._extract_data(paths, lambda path: os.path.getsize(path))

    def get_polars_dataframe(self, paths):
        '''polars_dataframe 생성'''
        df = pl.DataFrame({
            "full_path": paths,
            "file_id": self.extract_file_id(paths),
            "file_name": self.extract_file_name(paths),
            "folder_name": self.extract_folder_name(paths),
            "file_size": self.extract_file_size(paths)
        })
        return df
