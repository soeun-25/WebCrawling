import pandas as pd
import requests
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor

# CSV 파일 경로
csv_file_path = '2차/url_list.csv'

# CSV 파일 로드
df = pd.read_csv(csv_file_path)

# URL 유효성을 검사하는 함수
def check_url(url):
    if pd.isna(url):
        return url, 'Invalid'  # NaN 값은 유효하지 않은 URL로 처리

    prefixes = ['https://www.', 'http://www.', 'https://', 'http://']
    if 'www.' in url:
        prefixes = ['http://', 'https://'] + prefixes
    for prefix in prefixes:
        try:
            # 접두사가 이미 있다면 그대로 사용
            full_url = prefix + url if not url.startswith(('http://', 'https://')) else url
            response = requests.head(full_url, allow_redirects=True, timeout=5)
            if response.status_code == 200:
                return full_url, 'Valid'
            # HEAD 요청 실패 시 GET 요청으로 재시도
            response = requests.get(full_url, timeout=5)
            if response.status_code == 200:
                return full_url, 'Valid'
        except RequestException:
            continue  # 다음 접두사로 시도
    return url, 'Invalid'  # 모든 접두사를 시도한 후에도 유효하지 않음

# 멀티스레딩을 사용하여 모든 URL에 대한 유효성 검사를 수행
with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(check_url, df['Website']))

# 결과를 데이터프레임에 추가
for i, (full_url, validity) in enumerate(results):
    df.at[i, 'Website'] = full_url if validity == 'Valid' else df.at[i, 'Website']
    df.at[i, 'Valid/Invalid'] = validity

# 결과를 새로운 CSV 파일로 저장
validated_csv_file_path = 'validated_url_list_with_full_urls.csv'
df.to_csv(validated_csv_file_path, index=False)


