import csv
import json
from loguru import logger


class CsvStorage():
    def __init__(self):
        return
    
    def save_to_csv(self, data, csv_file_path) -> bool:
        # JSON 데이터를 파싱
        if isinstance(data, list):
            data_list = data

        # 'list' 키가 있는지 확인하고, 해당 값을 가져옴
        if isinstance(data, dict) and 'list' in data:
            data_list = data['list']

        if not data_list:
            logger.info('Failed to save')
            return False
        
        # CSV 파일에 데이터 저장
        with open(csv_file_path, 'a', newline='', encoding='utf-8') as file:
            if data_list:
                writer = csv.DictWriter(file, fieldnames=data_list[0].keys())
                
                # 파일이 비어있으면 헤더 작성
                if file.tell() == 0:
                    writer.writeheader()

                writer.writerows(data_list)
        logger.info('Success to save')
        return True

    def read_from_csv(self, csv_file_path) -> list[dict[str, str]]:
        data_list = []
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    data_list.append(row)
            logger.info("Success to read")
        except FileNotFoundError:
            logger.error("File not found")
        except Exception as e:
            logger.error(f"Error reading file: {e}")

        return data_list