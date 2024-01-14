import io
import json
import os
import zipfile
import requests
from loguru import logger
import xml.etree.ElementTree as ET

class Corp_code():
    corp_code: str
    corp_name: str
    stock_code: str
    modify_date: str


class DartAPI():
    # NOTE 환경 변수로 빼놓기
    api_key = "f25039bc5c5af07e0c9a788366ecfd58ca72674f"

    def __init__(self, api_key = "") -> None:
        self.api_key = api_key if api_key else self.api_key


    def _doenload_corp_code(self, url="", params={}) -> str:
        """
        Does the url contain a downloadable resource
        """

        if os.path.isfile(f"{os.getcwd()}/corp_code.xml"):
            logger.info('Return corp_code from local...')
            return f"{os.getcwd()}/corp_code.xml"
        
        response = requests.get(url, params)
        logger.info('Fetch corp_code from dart...')

        # NOTE 받은 컨텐츠를 바이트 스트림으로 변환
        zip_bytes = io.BytesIO(response.content)

        # NOTE ZIP 파일 열기
        with zipfile.ZipFile(zip_bytes, 'r') as zip_ref:
            # corp_code.xml 파일 찾아서 추출
            corp_code_xml = [f for f in zip_ref.namelist() if f.endswith('CORPCODE.xml')]
            if corp_code_xml:
                zip_ref.extract(corp_code_xml[0])
                # 필요한 경우, 파일명 변경 및 이동
                os.rename(corp_code_xml[0], 'corp_code.xml')
        return f"{os.getcwd()}/corp_code.xml"
    
    
    def _parse_xml_to_list(self, file_path):
        """
        Parse an XML file and convert the contents into a list of dictionaries.
        """
        tree = ET.parse(file_path)
        root = tree.getroot()

        # 결과를 저장할 리스트 초기화
        result_list = []

        # XML의 각 'list' 요소에 대해 반복
        for elem in root.findall('list'):
            corp_info = {
                'corp_code': elem.find('corp_code').text,
                'corp_name': elem.find('corp_name').text,
                'stock_code': elem.find('stock_code').text,
                'modify_date': elem.find('modify_date').text
            }
            result_list.append(corp_info)

        return result_list


    def fetch_corp_code(self) -> list[Corp_code]:
        """
        공시정보 목록 > 고유번호
        기업의 고유번호를 가지고 온다.
        """
        url = "https://opendart.fss.or.kr/api/corpCode.xml"

        # NOTE fetch corp_code data
        xml_path = self._doenload_corp_code(url, params={"crtfc_key":self.api_key})
        logger.info(f"Read to file :{xml_path}")

        # NOTE Load all corp_code
        self.corp_codes = self._parse_xml_to_list(xml_path)
        logger.info(f"Sample[corp_code.xml]: {self.corp_codes[:3]} ...")
        return self.corp_codes
    

    def single_company_fs(self, corp_code:str, bsns_year:str, reprt_code:str, fs_div = "OFS"):
        """
        상장기업 재무정보 목록 > 단일회사 전체 재무제표

        corp_code	고유번호	STRING(8)	Y	  공시대상회사의 고유번호(8자리) ※ 개발가이드 > 공시정보 > 고유번호 참고
        bsns_year	사업연도	STRING(4)	Y	  사업연도(4자리) ※ 2015년 이후 부터 정보제공
        reprt_code	보고서코드	 STRING(5)	 Y	   1분기보고서 : 11013
                                                반기보고서 : 11012
                                                3분기보고서 : 11014
                                                사업보고서 : 11011
        fs_div	개별/연결구분	STRING(3)	Y	   OFS:재무제표, CFS:연결재무제표
        """

        url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
        params = {"crtfc_key":self.api_key, "corp_code":corp_code, "bsns_year":bsns_year, "reprt_code":reprt_code, "fs_div":fs_div}
        logger.info(f"company_fs: {params}")
        response = requests.get(url, params)
        # logger.info(response.text)
        data = json.loads(response.text)
        
        if 'list' not in data:
            logger.info('fs is empty')
            return []
        
        # TODO check "status":"000"

        return data['list']
    
    
    def total_compnay_fs(self, bsns_year, reprt_code, fs_div = "OFS"):
        """
        상장기업 재무정보 목록 > 단일회사 전체 재무제표 (현 시점 존재하는 corp_code로 특정 기간의 재무재표를 받아본다.)

        bsns_year	사업연도	STRING(4)	Y	  사업연도(4자리) ※ 2015년 이후 부터 정보제공
        reprt_code	보고서코드	 STRING(5)	 Y	   1분기보고서 : 11013
                                                반기보고서 : 11012
                                                3분기보고서 : 11014
                                                사업보고서 : 11011
        fs_div	개별/연결구분	STRING(3)	Y	   OFS:재무제표, CFS:연결재무제표
        """
        return 