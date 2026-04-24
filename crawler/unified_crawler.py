import requests
import urllib3
import base64
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import json
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import collections
import subprocess
import os

# 전역 requests Session (연결 재사용으로 속도 향상)
GLOBAL_SESSION = requests.Session()
GLOBAL_SESSION.verify = False
GLOBAL_SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
})

# 병렬 처리 Worker 수
MAX_WORKERS = 5

# SSL Adapter for legacy servers (DH_KEY_TOO_SMALL)
class DESAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        import ssl
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        except:
            pass
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx
        )

# 설정
START_PAGE = 1
END_PAGE = 10          # jiwon 계열 크롤러 기본 페이지 수
SBIZ24_END_PAGE = 85   # 소상공인24 크롤링 페이지 수

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def crawl_bizinfo_list(driver):
    print(f"비즈인포(bizinfo.go.kr) 목록 크롤링 시작 (페이지 {START_PAGE} ~ {END_PAGE})...")
    all_data = []
    
    base_url = "https://www.bizinfo.go.kr/web/lay1/bbs/S1T122C128/AS/74/list.do?rows=15&cpage={}"
    
    for page in range(START_PAGE, END_PAGE + 1):
        url = base_url.format(page)
        print(f"\n--- 페이지 {page} 크롤링 중: {url} ---")
        
        try:
            driver.get(url)
            # 테이블 로딩 대기
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.CLASS_NAME, "table_Type_1"))
            )
            time.sleep(0.3)
            
            # BeautifulSoup으로 파싱 (목록은 정적 파싱이 빠름)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table_div = soup.find('div', class_='table_Type_1')
            
            if not table_div:
                print("테이블을 찾을 수 없습니다.")
                continue
                
            rows = table_div.find_all('tr')
            
            page_items = 0
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 5: # 번호, 지원분야, 사업명, 접수기간, 기관명 등
                    # 사업명 및 링크 (3번째 컬럼, 인덱스 2)
                    title_col = cols[2]
                    link_tag = title_col.find('a')
                    
                    if link_tag:
                        title = link_tag.text.strip()
                        href = link_tag.get('href')
                        # href가 상대경로일 경우 처리
                        # 예: view.do?pblancId=... 또는 /web/lay1/bbs/...
                        if href.startswith('/'):
                            link = f"https://www.bizinfo.go.kr{href}"
                        else:
                            # view.do... 형태인 경우
                            link = f"https://www.bizinfo.go.kr/web/lay1/bbs/S1T122C128/AS/74/{href}"
                            
                        # 접수기간 (4번째 컬럼, 인덱스 3)
                        period = cols[3].text.strip()
                        
                        row_data = {
                            '지원사업명': title,
                            '신청기간': period,
                            '링크': link,
                            '사업개요': '' # 상세 크롤링 전 빈 값
                        }
                        all_data.append(row_data)
                        page_items += 1
            
            print(f"페이지 {page}에서 {page_items}개의 항목을 찾았습니다.")
            
        except Exception as e:
            print(f"페이지 {page} 처리 중 오류 발생: {e}")
            
    return all_data

def crawl_bizinfo_details(driver, data):
    print(f"\n상세 내용('사업개요') 크롤링 시작 (총 {len(data)}개 항목)...")
    
    # 드라이버 재시작 주기
    RESTART_INTERVAL = 50
    
    for i, item in enumerate(data):
        # 주기적 재시작
        if i > 0 and i % RESTART_INTERVAL == 0:
            print(f"--- 드라이버 재시작 (메모리 관리, {i}/{len(data)}) ---")
            try:
                driver.quit()
            except:
                pass
            driver = setup_driver()
            
        link = item.get('링크')
        if not link or "javascript" in link: # 유효하지 않은 링크 건너뛰기
            item['사업개요'] = "링크 오류"
            continue
            
        print(f"[{i+1}/{len(data)}] 상세 크롤링 중: {item['지원사업명'][:20]}...")
        
        try:
            driver.get(link)
            # 페이지 로딩 대기 (view_cont가 나타날 때까지)
            try:
                WebDriverWait(driver, 7).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "view_cont"))
                )
            except:
                print("  - view_cont 요소를 찾을 수 없음 (Timeout)")
            
            time.sleep(0.4)  # 추가 대기
            
            content = ""
            
            # 방법 1: view_cont 내부의 li를 순회하며 '사업개요' 찾기 (가장 확실함)
            try:
                view_cont = driver.find_element(By.CLASS_NAME, "view_cont")
                lis = view_cont.find_elements(By.TAG_NAME, "li")
                
                for li in lis:
                    try:
                        title_span = li.find_element(By.CLASS_NAME, "s_title")
                        if "사업개요" in title_span.text:
                            txt_div = li.find_element(By.CLASS_NAME, "txt")
                            content = txt_div.text.strip()
                            break
                    except:
                        continue
            except Exception as e:
                print(f"  - 리스트 순회 중 오류: {e}")

            # 방법 2: XPath Fallback
            if not content:
                try:
                    content_div = driver.find_element(By.XPATH, "//span[@class='s_title'][contains(text(), '사업개요')]/following-sibling::div[@class='txt']")
                    content = content_div.text.strip()
                except:
                    pass
            
            if not content:
                content = "내용 없음 (추출 실패)"
                
            item['사업개요'] = content
            
        except Exception as e:
            print(f"상세 크롤링 중 치명적 오류 발생: {e}")
            item['사업개요'] = "크롤링 오류"
            
            # 세션 오류 시 재시작
            if "invalid session id" in str(e):
                try:
                    driver.quit()
                except:
                    pass
                driver = setup_driver()
                
        time.sleep(0.2)  # 부하 방지
        
    return data, driver


def crawl_btp_list(driver):
    print(f"\n부산테크노파크(btp.or.kr) 목록 크롤링 시작 (페이지 {START_PAGE} ~ {END_PAGE})...")
    all_data = []
    
    base_url = "https://www.btp.or.kr/kor/CMS/Board/Board.do?robot=Y&mCode=MN013&page={}"
    
    for page in range(START_PAGE, END_PAGE + 1):
        url = base_url.format(page)
        print(f"\n--- BTP 페이지 {page} 크롤링 중: {url} ---")
        
        try:
            driver.get(url)
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            time.sleep(0.3)
            
            rows = driver.find_elements(By.TAG_NAME, "tr")
            page_items = 0
            
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                # 컬럼: 번호, 사업공고명, 접수기간, 상태, 작성자, 게시일, 조회
                if len(cols) >= 4:
                    status = cols[3].text.strip()
                    
                    if "진행" in status:
                        title = cols[1].text.strip()
                        period = cols[2].text.strip()
                        
                        try:
                            link_tag = cols[1].find_element(By.TAG_NAME, "a")
                            link = link_tag.get_attribute("href")
                        except:
                            link = "링크 없음"

                        row_data = {
                            '지원사업명': title,
                            '신청기간': period,
                            '링크': link,
                            '사업개요': '',
                            '출처': '부산테크노파크' # 출처 구분 추가
                        }
                        all_data.append(row_data)
                        page_items += 1
            
            print(f"BTP 페이지 {page}에서 {page_items}개의 '진행' 항목을 찾았습니다.")
            
        except Exception as e:
            print(f"BTP 페이지 {page} 처리 중 오류 발생: {e}")
            
    return all_data

def crawl_btp_details(driver, data):
    print(f"\n부산테크노파크 상세 내용('사업개요') 크롤링 시작 (총 {len(data)}개 항목)...")
    
    for i, item in enumerate(data):
        link = item.get('링크')
        if not link or "javascript" in link:
            item['사업개요'] = "링크 오류"
            continue
            
        print(f"[{i+1}/{len(data)}] BTP 상세 크롤링 중: {item['지원사업명'][:20]}...")
        
        try:
            driver.get(link)
            try:
                WebDriverWait(driver, 7).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "board-biz-info"))
                )
            except:
                print("  - board-biz-info 요소를 찾을 수 없음 (Timeout)")
            
            time.sleep(0.4)
            
            content_list = []
            
            try:
                info_div = driver.find_element(By.CLASS_NAME, "board-biz-info")
                lis = info_div.find_elements(By.TAG_NAME, "li")
                
                for li in lis:
                    try:
                        tit_span = li.find_element(By.CLASS_NAME, "tit")
                        tit_text = tit_span.text.strip().replace(" ", "")
                        
                        if any(keyword in tit_text for keyword in ["지원대상", "지원내용", "사업대상", "사업내용"]):
                            # 제목(span.tit)을 제외한 순수 텍스트 추출은 복잡하므로 전체 텍스트 사용 후 정리
                            full_text = li.text.strip()
                            # 단순하게 줄바꿈으로 구분하여 추가
                            content_list.append(full_text)
                    except:
                        continue
            except Exception as e:
                print(f"  - 상세 내용 추출 중 오류: {e}")
            
            item['사업개요'] = "\n\n".join(content_list) if content_list else "내용 없음"
            
        except Exception as e:
            print(f"BTP 상세 크롤링 중 오류 발생: {e}")
            item['사업개요'] = "크롤링 오류"
            
    return data

LOG_FILE = os.path.join(os.path.dirname(__file__), 'crawl_history.log')

def write_crawl_log(total_count, source_counts):
    """크롤링 실행 로그를 누적 기록"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"[{timestamp}] 크롤링 실행 완료 | 총 {total_count}개 항목 수집",
    ]
    for source, count in sorted(source_counts.items()):
        lines.append(f"  - {source}: {count}개")
    lines.append("")  # 빈 줄 구분

    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write("\n".join(lines) + "\n")

    print(f"로그 기록 완료: {LOG_FILE}")

# ── 추천 정렬용 키워드/패턴 ─────────────────────────────
# 지원사업명만으로 '기업에게 직접적 지원(금전·재정·바우처 등)'이 드러나는 단어
DIRECT_SUPPORT_KEYWORDS_HIGH = (
    '자금', '융자', '대출', '이차보전', '보조금', '지원금', '출연금',
    '기술료', '바우처', '사업화자금', '정책자금', '장려금', '금융지원',
    '현금지원', '환급', '세제지원', '세액공제', '투자유치', '보증',
)
# 직접 금전 지원은 아니지만 실질적 기업 혜택이 있는 프로그램
DIRECT_SUPPORT_KEYWORDS_MID = (
    'R&D', '연구개발', '컨설팅', '멘토링', '인큐베이팅', '액셀러레이팅',
    '시설지원', '장비', '공간지원', '판로', '마케팅', '해외진출',
    '수출', '특허', '지재권', '시제품', '시작품', '스케일업',
)
BAD_OVERVIEW_PATTERNS = ('내용 없음', '크롤링 오류', '내용 추출 실패', '링크 오류')
UNLIMITED_PATTERNS = ('예산 소진', '소진시까지', '소진 시까지', '예산소진')


def _direct_support_score(name: str) -> int:
    """지원사업명으로 추정한 직접 지원 정도. 2=재정/바우처, 1=간접혜택, 0=불명"""
    if not name:
        return 0
    for kw in DIRECT_SUPPORT_KEYWORDS_HIGH:
        if kw in name:
            return 2
    for kw in DIRECT_SUPPORT_KEYWORDS_MID:
        if kw in name:
            return 1
    return 0


def _overview_quality(overview: str) -> int:
    """사업개요 상세도. 2=상세(≥200자), 1=간단(≥80자), 0=없음/비정상"""
    clean = (overview or '').replace('\u200b', '').strip()
    if not clean:
        return 0
    head = clean[:40]
    if any(bad in head for bad in BAD_OVERVIEW_PATTERNS):
        return 0
    length = len(clean)
    if length >= 200:
        return 2
    if length >= 80:
        return 1
    return 0


def _period_score(period: str, today=None):
    """신청기간 여유 평가.
    반환: (bucket, days_left, is_expired)
      bucket: 4=예산소진 · 3=30일↑ · 2=14일↑ · 1=7일↑ · 0=0일↑ · -1=마감/미상
    """
    period = (period or '').replace('\n', ' ').strip()
    if not period:
        return (-1, -9999, False)
    if any(k in period for k in UNLIMITED_PATTERNS):
        return (4, 9999, False)
    today = today or datetime.now().date()
    dates = re.findall(r'(\d{4})[-./](\d{1,2})[-./](\d{1,2})', period)
    if not dates:
        return (-1, -9999, False)
    y, m, d = dates[-1]
    try:
        end_date = datetime(int(y), int(m), int(d)).date()
    except ValueError:
        return (-1, -9999, False)
    days = (end_date - today).days
    if days < 0:
        return (-1, days, True)
    if days >= 30:
        return (3, days, False)
    if days >= 14:
        return (2, days, False)
    if days >= 7:
        return (1, days, False)
    return (0, days, False)


def _sort_key(item):
    """
    추천 정렬 기준 (ascending 정렬 → 스코어 음수화):
      0) 마감된 항목은 무조건 하단
      1) 지원사업명에 '직접 지원(자금·보조금·바우처 등)' 포함 여부
      2) 사업개요 상세도 (길이 기반)
      3) 신청기간 여유 버킷 (예산소진 > 30일↑ > 14일↑ > 7일↑)
      4) 실제 남은 일수 (동일 버킷 내에서 여유 많은 카드 우선)
    """
    name     = (item.get('지원사업명') or '').strip()
    overview = item.get('사업개요') or ''
    period   = item.get('신청기간') or ''

    support_score  = _direct_support_score(name)
    overview_score = _overview_quality(overview)
    bucket, days_left, is_expired = _period_score(period)

    return (
        1 if is_expired else 0,   # 마감 → 하단
        -support_score,           # 직접 지원 강도
        -overview_score,          # 사업개요 상세도
        -bucket,                  # 신청기간 여유 버킷
        -days_left,               # 남은 일수
    )

def save_to_web_json(data):
    """크롤링 결과를 웹 뷰어용 data.json에만 저장.

    - 각 카드에 `first_seen` 필드를 태깅하여 '신규 카드' 식별 가능하게 함.
    - 기존 data.json에 있던 링크는 기존 first_seen 값을 그대로 보존.
    - 첫 실행(기존 data.json 없음) 시 모든 카드가 신규로 잡히는 것을 방지하기
      위해 전체를 어제 날짜로 태깅.
    """
    if not data:
        print("저장할 데이터가 없습니다.")
        return 0

    # 링크 기준 전역 중복 제거 (크롤러별 중복 방지 실패 시 최종 방어선)
    seen = set()
    deduped = []
    for item in data:
        key = item.get('링크', '') or item.get('지원사업명', '')
        if key and key not in seen:
            seen.add(key)
            deduped.append(item)
    if len(deduped) < len(data):
        print(f"[중복 제거] {len(data)}개 → {len(deduped)}개 (중복 {len(data) - len(deduped)}개 제거)")
    data = deduped

    # ── first_seen 태깅 ──────────────────────────────────
    web_data_path = os.path.join(os.path.dirname(__file__), 'biz_support_web', 'data.json')
    existing_first_seen = {}
    had_previous = False
    try:
        with open(web_data_path, 'r', encoding='utf-8') as f:
            existing = json.load(f)
            for it in existing.get('data', []):
                link = it.get('링크')
                fs = it.get('first_seen')
                if link and fs:
                    existing_first_seen[link] = fs
            had_previous = bool(existing.get('data'))
    except (FileNotFoundError, json.JSONDecodeError):
        had_previous = False

    today_str = datetime.now().strftime("%Y-%m-%d")
    # 첫 실행에서는 모든 카드를 "이미 존재하던 것"으로 취급 (어제 날짜 태깅)
    fallback_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    new_count = 0
    for item in data:
        link = item.get('링크', '')
        if link in existing_first_seen:
            item['first_seen'] = existing_first_seen[link]
        elif had_previous:
            item['first_seen'] = today_str
            new_count += 1
        else:
            item['first_seen'] = fallback_str
    print(f"[first_seen] 신규 {new_count}개 / 기존 {len(data) - new_count}개")

    # 정렬 적용
    sorted_data = sorted(data, key=_sort_key)

    # 웹 뷰어 데이터 덮어쓰기 (last_updated 메타데이터 포함)
    try:
        web_payload = {
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data": sorted_data
        }
        with open(web_data_path, 'w', encoding='utf-8') as f:
            json.dump(web_payload, f, ensure_ascii=False, indent=2)
        print(f"웹 뷰어용 '{web_data_path}' 파일을 성공적으로 갱신했습니다. (last_updated + first_seen 포함)")
    except Exception as e:
        print(f"[오류] 웹 뷰어 데이터 갱신 실패: {e}")

    return len(sorted_data)


def crawl_dgtp_list(end_page=3):
    """
    대구테크노파크(DGTP) 목록 크롤링
    URL: https://dgtp.or.kr/bbs/BoardControll.do?bbsId=BBSMSTR_000000000003
    필터: 상태 == '접수중'
    """
    print(f"\n[대구테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    base_url = "https://dgtp.or.kr/bbs/BoardControll.do"
    results = []
    
    # requests를 사용하여 목록 크롤링 (속도 효율성)
    for page in range(1, end_page + 1):
        try:
            params = {
                'bbsId': 'BBSMSTR_000000000003',
                'pageIndex': page
            }
            response = GLOBAL_SESSION.get(base_url, params=params)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            rows = soup.select('table tbody tr')
            if not rows:
                print(f"  {page}페이지: 게시물이 없습니다.")
                continue
                
            print(f"  {page}페이지 크롤링 중... ({len(rows)}개 게시물)")
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 5:
                    continue
                
                # 번호, 부서명, 제목, 접수기간, 상태, ...
                status = cols[4].get_text(strip=True)
                
                if status == '접수중':
                    title_elem = cols[2].find('a')
                    if not title_elem:
                        continue
                        
                    title = title_elem.get_text(strip=True)
                    period = cols[3].get_text(strip=True)
                    
                    link_href = title_elem.get('href')
                    full_link = None

                    # onclick에서 nttId 추출: fn_egov_inqire_notice('15103', 'BBSMSTR_000000000003', '010351')
                    onclick = title_elem.get('onclick')
                    if onclick:

                        # nttId가 첫 번째 인자로 추정됨
                        match = re.search(r"fn_egov_inqire_notice\('(\d+)'", onclick)
                        if match:
                            ntt_id = match.group(1)
                            # 상세 페이지 URL 구성: JS function fn_egov_inqire_notice references /bbs/BoardControllView.do
                            # params: nttId, bbsId. (others like pageIndex, command might be optional or ignored)
                            full_link = f"https://dgtp.or.kr/bbs/BoardControllView.do?bbsId=BBSMSTR_000000000003&nttId={ntt_id}"
                    
                    # 만약 onclick 파싱 실패시 href 확인 (백업)
                    if not full_link and link_href and 'http' in link_href:
                         full_link = link_href

                    if not full_link:
                         full_link = f"링크 파싱 실패 (onclick: {onclick})"

                    results.append({
                        '출처': '대구테크노파크',
                        '지원사업명': title,
                        '신청기간': period,
                        '링크': full_link
                    })
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중 오류 발생: {e}")
            
    print(f"[대구테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results

def crawl_dgtp_details(data_list, driver):
    """
    대구테크노파크 상세 페이지에서 '사업개요' 추출
    """
    print("\n[대구테크노파크] 상세 페이지에서 '사업개요' 추출 시작...")
    
    # 키워드 리스트 (띄어쓰기 유무 포함)
    keywords = ["지원대상", "지 원 대 상", "지원분야", "지 원 분 야", "지원내용", "지 원 내 용"]
    
    for idx, item in enumerate(data_list):
        link = item.get('link') or item.get('링크')
        if not link:
            continue
            
        print(f"  [{idx+1}/{len(data_list)}] 상세 페이지 이동: {link}")
        
        try:
            driver.get(link)
            # 페이지 로딩 대기
            time.sleep(1)
            
            # 내용을 담을 리스트
            contents = []
            
            # ul > li 구조 내에서 키워드 찾기
            # ul > li 구조 내에서 키워드 찾기
            # Container class가 없거나 다를 수 있으므로, 키워드를 포함하는 span을 가진 li를 직접 찾음
            try:
                # 키워드별로 XPath 생성
                xpath_parts = []
                for kw in keywords:
                    xpath_parts.append(f"contains(text(), '{kw}')")
                
                # 통합 XPath: //li[.//span[contains(text(), '지원대상') or ...]]
                or_clause = " or ".join(xpath_parts)
                xpath = f"//li[.//span[{or_clause}]]"
                
                list_items = driver.find_elements(By.XPATH, xpath)

                for li in list_items:
                    li_text = li.text.strip()
                    clean_text = li_text
                    
                    # 키워드 제거 시도
                    for kw in keywords:
                        if kw in clean_text:
                            # "지 원 대 상" 제거
                            clean_text = clean_text.replace(kw, "").strip()
                            # 혹시 모를 특수문자 제거 (:, .)
                            if clean_text.startswith(":") or clean_text.startswith("."):
                                clean_text = clean_text[1:].strip()
                            break
                            
                    contents.append(clean_text)
                            
            except Exception as e:
                pass
            
            # 만약 contents가 비어있다면 전체 텍스트에서 일부라도 가져오도록 시도하거나 '내용 없음' 처리
            if contents:
                full_overview = "\n".join(contents)
            else:
                full_overview = "내용 없음 (상세 페이지 참조)"
                
            item['사업개요'] = full_overview
            print(f"    -> 사업개요 추출 완료 ({len(full_overview)}자)")
            
        except Exception as e:
            print(f"    [오류] 상세 페이지 접속/파싱 실패: {e}")
            item['사업개요'] = "오류 발생"
            
        # 적당한 딜레이
        time.sleep(1)
            
    return data_list, driver

def crawl_itp_list(driver):
    """
    인천테크노파크(itp.or.kr) 게시판 크롤링
    URL: https://www.itp.or.kr/intro.asp?tmid=13
    - Selenium 사용 (페이지네이션이 JS로 처리됨)
    - 진행상태가 '진행중'인 항목만 추출
    - 상세 내용은 크롤링하지 않음 (빈칸)
    """
    print(f"\n인천테크노파크(itp.or.kr) 목록 크롤링 시작...")
    
    base_url = "https://www.itp.or.kr/intro.asp?tmid=13"
    results = []
    
    try:
        driver.get(base_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
        )
        
        # 페이지 순회 (최대 10페이지 or '진행중' 없을 때까지)
        # 하지만 사용자가 '진행중'인 목록이 6페이지까지 있다고 했으므로,
        # 일단 페이지네이션 링크를 찾아서 클릭하며 진행.
        
        current_page = 1
        max_page_check = 10 # 안전장치
        
        while current_page <= max_page_check:
            print(f"--- ITP 페이지 {current_page} 분석 중 ---")
            
            # 행 분석
            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            page_found_count = 0
            
            for row in rows:
                try:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) < 5:
                        continue
                        
                    # 1. 상태 확인 (이미지 alt 속성)
                    try:
                        status_img = cols[4].find_element(By.TAG_NAME, "img")
                        status = status_img.get_attribute("alt").strip()
                    except:
                        status = "상태알수없음"
                        
                    if status != "진행중":
                        continue
                        
                    # 2. 데이터 추출
                    # 번호(0), 분야(1), 제목(2), 작성일(3), 진행상태(4), 조회수(5)
                    title_elem = cols[2]
                    title = title_elem.text.strip()
                    date = cols[3].text.strip()
                    
                    # 링크 추출 (javascript:fncShow('id') 형태)
                    try:
                        link_a = title_elem.find_element(By.TAG_NAME, "a")
                        href = link_a.get_attribute("href")
                        
                        full_link = href # 기본값
                        
                        # href="javascript:fncShow('10175')" -> https://www.itp.or.kr/intro.asp?tmid=13&seq=10175
                        if "fncShow" in href:
                             match = re.search(r"fncShow\('(\d+)'\)", href)
                             if match:
                                 seq = match.group(1)
                                 full_link = f"https://www.itp.or.kr/intro.asp?tmid=13&seq={seq}"
                    except:
                        full_link = "링크 없음"

                    results.append({
                        '출처': '인천테크노파크',
                        '지원사업명': title,
                        '신청기간': date, # ITP는 접수기간 대신 작성일만 있음. 또는 상세에 있을 수 있으나 상세 크롤링 안함.
                        '링크': full_link,
                        '사업개요': '' # 상세 내용 크롤링 안함 (요청사항)
                    })
                    page_found_count += 1
                    
                except Exception as row_e:
                    print(f"  [오류] ITP 행 분석 중: {row_e}")
                    continue
            
            print(f"  -> {page_found_count}개의 '진행중' 항목 추출")
            
            # 다음 페이지 이동
            # 페이지네이션 영역 찾기
            # <div class="paging"> ... <a href="javascript:fncBoardPage(2)">2</a> ... </div>
            # 현재 페이지 + 1 인 링크를 찾아서 클릭
            
            next_page = current_page + 1
            try:
                # 정확히 텍스트가 next_page인 링크 찾기 (1, 2, 3...)
                # XPath: //div[contains(@class, 'paging') or contains(@class, 'page')]//a[text()='2']
                next_link_xpath = f"//div[contains(@class, 'page') or contains(@class, 'paging')]//a[normalize-space(text())='{next_page}']"
                next_link = driver.find_element(By.XPATH, next_link_xpath)
                
                # 클릭
                driver.execute_script("arguments[0].click();", next_link)
                time.sleep(1)  # 이동 대기
                current_page = next_page
                
            except Exception as e:
                print(f"  -> {next_page}페이지로 이동 불가: {e}")
                break
                
    except Exception as e:
        print(f"[오류] 인천테크노파크 크롤링 중: {e}")
        
    return results


def crawl_gjtp_list(end_page=10):
    """
    광주테크노파크(GJTP) 목록 크롤링
    URL: https://www.gjtp.or.kr/home/business.cs?pageIndex={page}
    필터: 접수상태 == '접수중'
    """
    print(f"\n[광주테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    base_url = "https://www.gjtp.or.kr/home/business.cs"
    results = []
    
    for page in range(1, end_page + 1):
        try:
            params = {'pageIndex': page}
            response = GLOBAL_SESSION.get(base_url, params=params)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            table = soup.find('table')
            if not table:
                print(f"  {page}페이지: 테이블을 찾을 수 없습니다.")
                continue
                
            rows = table.find_all('tr')[1:] # 헤더 제외
            if not rows:
                print(f"  {page}페이지: 게시물이 없습니다.")
                continue
                
            print(f"  {page}페이지 크롤링 중... ({len(rows)}개 게시물)")
            page_found = 0
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 5:
                    continue
                
                # 컬럼 매핑: 0:제목, 1:기간, 2:담당자, 3:조회수, 4:상태
                status = cols[4].text.strip()
                
                if status == '접수중':
                    title_tag = cols[0].find('a')
                    if not title_tag:
                        continue
                        
                    title = title_tag.text.strip()
                    period = cols[1].text.strip()
                    href = title_tag.get('href')
                    
                    if href:
                        if href.startswith('?'):
                            full_link = f"https://www.gjtp.or.kr/home/business.cs{href}"
                        elif href.startswith('/'):
                            full_link = f"https://www.gjtp.or.kr{href}"
                        else:
                            full_link = f"https://www.gjtp.or.kr/home/{href}"
                            
                        results.append({
                            '출처': '광주테크노파크',
                            '지원사업명': title,
                            '신청기간': period,
                            '링크': full_link
                        })
                        page_found += 1
            
            if page_found == 0 and len(results) > 0 and page > 1:
                # 만약 이전 페이지에서 데이터를 찾았는데 이번 페이지에서 없다면,
                # 더 이상 '접수중' 데이터가 없을 가능성이 높으므로 로직에 따라 중단할 수 있음.
                pass
                
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[광주테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results

def crawl_gjtp_details(data_list):
    """
    광주테크노파크 상세 페이지에서 '사업목적' 추출 -> '사업개요'에 저장
    """
    print(f"\n[광주테크노파크] 상세 페이지 병렬 크롤링 시작 (총 {len(data_list)}개)...")

    def _fetch_gjtp(item):
        link = item.get('링크')
        if not link:
            item['사업개요'] = ''
            return item
        try:
            response = GLOBAL_SESSION.get(link, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            target_th = soup.find('th', string=lambda t: t and '사업목적' in t.strip())
            content = ""
            if target_th:
                td = target_th.find_next_sibling('td')
                if td:
                    content = td.text.strip()
            item['사업개요'] = content or "내용 없음 (사업목적 찾지 못함)"
        except Exception as e:
            item['사업개요'] = "크롤링 오류"
        return item

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(_fetch_gjtp, data_list))

    return results


def crawl_djtp_list(end_page=10):
    """
    대전테크노파크(DJTP) 목록 크롤링
    URL: https://www.djtp.or.kr/pbanc?mid=a20101000000&nPage={page}
    필터: 사업신청 == '신청'
    """
    print(f"\n[대전테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    base_url = "https://www.djtp.or.kr/pbanc"
    results = []
    
    for page in range(1, end_page + 1):
        try:
            params = {
                'mid': 'a20101000000',
                'nPage': page
            }
            response = GLOBAL_SESSION.get(base_url, params=params)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 테이블 찾기: '사업신청' 헤더가 있는 테이블 찾기
            target_table = None
            tables = soup.find_all('table')
            for table in tables:
                headers = [th.text.strip() for th in table.find_all('th')]
                if "사업신청" in headers:
                    target_table = table
                    break
            
            if not target_table:
                print(f"  {page}페이지: 테이블을 찾을 수 없습니다.")
                continue
                
            rows = target_table.find_all('tr')[1:] # 헤더 제외
            if not rows:
                print(f"  {page}페이지: 게시물이 없습니다.")
                continue
                
            print(f"  {page}페이지 크롤링 중... ({len(rows)}개 게시물)")
            page_found = 0
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 5:
                    continue
                
                # 컬럼 매핑: 0:번호, 1:유형, 2:공고명, 3:사업신청, 4:접수기간, 5:부서, 6:조회수
                # 헤더: ['번호', '유형', '공고명', '사업신청', '접수기간', '부서', '조회수']
                
                status = cols[3].text.strip()
                
                if '신청' in status:
                    title_tag = cols[2].find('a')
                    if not title_tag:
                        continue
                        
                    title = title_tag.text.strip()
                    period = cols[4].text.strip()
                    href = title_tag.get('href')
                    
                    full_link = ""
                    if href:
                        if href.startswith('http'):
                            full_link = href
                        elif href.startswith('/'):
                            full_link = f"https://www.djtp.or.kr{href}"
                        else:
                            full_link = f"https://www.djtp.or.kr/pbanc/{href}"
                            
                    results.append({
                        '출처': '대전테크노파크',
                        '지원사업명': title,
                        '신청기간': period,
                        '링크': full_link,
                        '사업개요': '' # 요청에 따라 빈칸
                    })
                    page_found += 1
            
            if page_found == 0 and len(results) > 0 and page > 1:
                pass
                
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[대전테크노파크] 총 {len(results)}개의 '신청' 공고 추출 완료")
    return results

def crawl_utp_list(driver, end_page=10):
    """
    울산테크노파크(UTP) 목록 크롤링 (Selenium)
    URL: https://www.utp.or.kr/include/contents.php?mnuno=M0000018&menu_group=1&sno=0102&task=list&page={page}
    필터: 상태 == '접수중'
    """
    print(f"\n[울산테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    base_url = "https://www.utp.or.kr/include/contents.php?mnuno=M0000018&menu_group=1&sno=0102&task=list"
    results = []
    
    for page in range(1, end_page + 1):
        try:
            url = f"{base_url}&page={page}"
            driver.get(url)
            time.sleep(1)  # 페이지 로드 대기
            
            # 테이블 찾기
            try:
                # 텍스트로 '제목'과 '접수기간'이 포함된 테이블 찾기 (XPath로 단순화)
                # //table[.//th[contains(text(), '상태')]]
                table_xpath = "//table[.//th[contains(text(), '상태')]]"
                table = driver.find_element(By.XPATH, table_xpath)
            except:
                print(f"  {page}페이지: 테이블을 찾을 수 없습니다.")
                continue
            
            rows = table.find_elements(By.TAG_NAME, "tr")
            # 헤더(th 포함 행) 제외
            data_rows = [row for row in rows if not row.find_elements(By.TAG_NAME, "th")]
            
            if not data_rows:
                print(f"  {page}페이지: 게시물이 없습니다.")
                continue
                
            print(f"  {page}페이지 크롤링 중... ({len(data_rows)}개 게시물)")
            count = 0 
            
            for row in data_rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                # 순번, 제목, 접수기간, 상태, 게시일, 조회
                if len(cols) < 4:
                    continue
                    
                status = cols[3].text.strip() # 4번째 컬럼
                
                if status == '접수중':
                    try:
                        title_el = cols[1].find_element(By.TAG_NAME, "a")
                        title = title_el.text.strip()
                        period = cols[2].text.strip()
                        
                        # data-seq 추출하여 상세 URL 생성
                        seq = title_el.get_attribute('data-seq')
                        if seq:
                            detail_url = f"https://www.utp.or.kr/include/contents.php?mnuno=M0000018&menu_group=1&sno=0102&task=view&seq={seq}"
                            
                            results.append({
                                '출처': '울산테크노파크',
                                '지원사업명': title,
                                '신청기간': period,
                                '링크': detail_url
                            })
                            count += 1
                    except Exception as e:
                        print(f"    [오류] 항목 파싱 실패: {e}")
                        
            if count == 0 and len(results) > 0 and page > 1:
                # 이전 페이지엔 있었으나 이번엔 없으면 종료 가능
                pass
                
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[울산테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results

def crawl_utp_details(data_list, driver):
    """
    울산테크노파크 상세 페이지에서 '지원개요' 및 '지원대상' 추출 -> '사업개요'에 저장
    구조: li > span.text('지원개요') + p 
    """
    print(f"\n[울산테크노파크] 상세 페이지 크롤링 시작 (총 {len(data_list)}개)...")
    
    for i, item in enumerate(data_list):
        link = item.get('링크')
        if not link:
            continue
            
        print(f"  [{i+1}/{len(data_list)}] 상세 크롤링: {item['지원사업명'][:20]}...")
        
        try:
            driver.get(link)
            time.sleep(0.8)
            
            overview = []
            
            # 1. 지원개요
            try:
                # //li[.//span[contains(text(), '지원개요')]]
                # 그 안의 p 태그나 형제 요소
                target_li = driver.find_elements(By.XPATH, "//li[.//span[contains(text(), '지원개요')]]")
                if target_li:
                     # li 안에 p가 있는지 확인
                     p_tags = target_li[0].find_elements(By.TAG_NAME, "p")
                     for p in p_tags:
                         txt = p.text.strip()
                         if txt:
                             overview.append(f"[지원개요] {txt}")
            except:
                pass

            # 2. 지원대상
            try:
                target_li = driver.find_elements(By.XPATH, "//li[.//span[contains(text(), '지원대상')]]")
                if target_li:
                     p_tags = target_li[0].find_elements(By.TAG_NAME, "p")
                     for p in p_tags:
                         txt = p.text.strip()
                         if txt:
                             overview.append(f"[지원대상] {txt}")
            except:
                pass
            
            if not overview:
                item['사업개요'] = "내용 없음 (형식 불일치)"
            else:
                item['사업개요'] = "\n".join(overview)
                
        except Exception as e:
            print(f"    [오류] 상세 페이지 이동/파싱 실패: {e}")
            item['사업개요'] = "크롤링 오류"
            
    return data_list

def crawl_sjtp_list(end_page=5):
    """
    세종테크노파크(SJTP) 목록 크롤링 (Requests)
    URL: https://sjtp.or.kr/bbs/board.php?bo_table=business01&page={page}
    필터: 진행상태 == '접수중'
    상세 내용: 없음 (빈칸)
    """
    print(f"\n[세종테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    base_url = "https://sjtp.or.kr/bbs/board.php"
    results = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for page in range(1, end_page + 1):
        try:
            params = {
                'bo_table': 'business01',
                'page': page
            }
            response = GLOBAL_SESSION.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 테이블 찾기: '진행상태'가 포함된 첫 번째 테이블
            tables = soup.find_all('table')
            target_table = None
            for table in tables:
                if "진행상태" in table.text:
                    target_table = table
                    break
            
            if not target_table:
                print(f"  {page}페이지: 테이블을 찾을 수 없습니다.")
                continue
                
            rows = target_table.find_all('tr')[1:] # 헤더 스킵
            
            if not rows:
                print(f"  {page}페이지: 게시물이 없습니다.")
                continue
                
            print(f"  {page}페이지 크롤링 중... ({len(rows)}개 게시물)")
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 4:
                    continue
                
                # 진행상태 (마지막 컬럼)
                status = cols[-1].text.strip()
                
                if status == '접수중':
                    try:
                        title_col = cols[1]
                        a_tag = title_col.find('a')
                        if not a_tag:
                            continue
                            
                        title = a_tag.text.strip()
                        link = a_tag.get('href')
                        
                        # 날짜 추출: '신청기간' 텍스트 이후
                        col_text = title_col.text
                        period = ""
                        if "신청기간" in col_text:
                            period_part = col_text.split("신청기간")[-1]
                            period = period_part.strip()
                        
                        results.append({
                            '출처': '세종테크노파크',
                            '지원사업명': title,
                            '신청기간': period,
                            '링크': link,
                            '사업개요': '' # 상세 내용 없음
                        })
                    except Exception as e:
                        print(f"    [오류] 항목 파싱 실패: {e}")
                        
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[세종테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results

def crawl_gtp_list(end_page=5):
    """
    경기테크노파크(GTP) 목록 크롤링 (Requests)
    URL: https://pms.gtp.or.kr/web/business/webBusinessThemeList.do
    필터: 상태 == '접수중'
    상세 내용: 없음 (빈칸)
    """
    print(f"\n[경기테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    base_url = "https://pms.gtp.or.kr/web/business/webBusinessThemeList.do"
    results = []
    seen_links = set()  # 중복 방지

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for page in range(1, end_page + 1):
        try:
            params = {
                'pageIndex': page
            }
            response = GLOBAL_SESSION.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # 테이블 찾기: '공고 제목'과 '상태'가 포함된 테이블
            tables = soup.find_all('table')
            target_table = None
            for table in tables:
                if "공고 제목" in table.text and "상태" in table.text:
                    target_table = table
                    break

            if not target_table:
                print(f"  {page}페이지: 테이블을 찾을 수 없습니다.")
                continue

            rows = target_table.find_all('tr')[1:] # 헤더 스킵

            if not rows:
                print(f"  {page}페이지: 게시물이 없습니다.")
                continue

            print(f"  {page}페이지 크롤링 중... ({len(rows)}개 게시물)")
            page_new_items = 0

            for row in rows:
                cols = row.find_all('td')
                # 예상: No(0), 제목(1), 유형(2), 지역(3), 주최(4), 기간(5), 상태(6)
                if len(cols) < 7:
                    continue

                # 상태 (마지막 컬럼)
                status = cols[-1].text.strip()

                if status == '접수중':
                    try:
                        title_col = cols[1]
                        a_tag = title_col.find('a')
                        if not a_tag:
                            continue

                        title = a_tag.text.strip()

                        # 링크 생성 (onclick 파싱)
                        onclick = a_tag.get('onclick', '')
                        link = ""
                        if "fn_goView" in onclick:
                            # fn_goView('172142'); -> '172142' 추출
                            b_idx = onclick.split("'")[1]
                            link = f"https://pms.gtp.or.kr/web/business/webBusinessView.do?b_idx={b_idx}"
                        else:
                            link = base_url

                        # 이미 수집한 링크면 건너뜀
                        if link in seen_links:
                            continue
                        seen_links.add(link)
                        page_new_items += 1

                        # 기간 (인덱스 5)
                        period = cols[5].text.strip()

                        results.append({
                            '출처': '경기테크노파크',
                            '지원사업명': title,
                            '신청기간': period,
                            '링크': link,
                            '사업개요': '' # 상세 내용 없음
                        })
                    except Exception as e:
                        print(f"    [오류] 항목 파싱 실패: {e}")

            # 페이지에서 신규 항목이 없으면 서버가 페이지를 지원하지 않는 것 → 조기 종료
            if page > 1 and page_new_items == 0:
                print(f"  [경기테크노파크] 페이지 {page}에서 신규 항목 없음 → 크롤링 종료")
                break

        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")

    print(f"[경기테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료 (고유)")
    return results

def crawl_gdtp_list(end_page=5):
    """
    경기대진테크노파크(GDTP) 목록 크롤링 (Requests)
    URL: https://www.gdtp.or.kr/board/announcement?&page=1
    필터: 제목에 '[ 진행중 ]' 포함
    """
    print(f"\n[경기대진테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    base_url = "https://www.gdtp.or.kr/board/announcement"
    results = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for page in range(1, end_page + 1):
        try:
            params = {
                'page': page
            }
            response = GLOBAL_SESSION.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            rows = soup.select('div.tbl.type_bbs div.tbody div.colgroup')
            if not rows:
                print(f"  {page}페이지: 게시물이 없습니다.")
                continue
                
            print(f"  {page}페이지 크롤링 중... ({len(rows)}개 항목 검사)")
            
            page_results = []
            
            for row in rows:
                try:
                    # 제목 및 링크
                    title_tag = row.select_one('div.btitle h3 a')
                    if not title_tag:
                        continue
                    
                    title = title_tag.text.strip()
                    link = title_tag.get('href')

                    # 필터링: [ 진행중 ]
                    if "[ 진행중 ]" not in title:
                        continue

                    # 링크에서 ?page= 파라미터 제거하여 중복 방지
                    # /post/3176?page=1 → /post/3176
                    link_clean = re.sub(r'\?page=\d+', '', link)

                    # 날짜 (YY-MM-DD -> YYYY-MM-DD) — 목록의 등록일 (상세에서 공고기간으로 교체됨)
                    date_tag = row.select_one('div.bdate span')
                    date_text = ""
                    if date_tag:
                        raw_date = date_tag.get_text().strip()
                        if raw_date and raw_date.count('-') == 2:
                            parts = raw_date.split('-')
                            if len(parts[0]) == 2:
                                date_text = f"20{parts[0]}-{parts[1]}-{parts[2]}"
                            else:
                                date_text = raw_date

                    # 중복 제거 (정규화된 링크 기준)
                    if any(re.sub(r'\?page=\d+', '', r['링크']) == link_clean for r in results) or \
                       any(re.sub(r'\?page=\d+', '', r['링크']) == link_clean for r in page_results):
                        continue

                    page_results.append({
                        '출처': '경기대진테크노파크',
                        '지원사업명': title,
                        '신청기간': date_text, # 상세 크롤링에서 공고기간으로 교체됨
                        '링크': link_clean,
                        '사업개요': '' # 상세에서 계속
                    })
                    
                except Exception as e:
                    print(f"    [오류] 항목 파싱 실패: {e}")
            
            results.extend(page_results)
                        
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[경기대진테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results

def crawl_gdtp_details(data_list):
    """
    경기대진테크노파크 상세 페이지 크롤링 (병렬)
    대상: div#post-content (사업개요), li.list-group-item (공고기간)
    """
    print(f"[경기대진테크노파크] {len(data_list)}개 상세 페이지 병렬 크롤링 시작...")
    _hdrs = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

    def _fetch(item):
        try:
            r = GLOBAL_SESSION.get(item['링크'], headers=_hdrs, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')

            # 사업개요 추출
            div = soup.find('div', id='post-content')
            item['사업개요'] = div.get_text(separator='\n', strip=True) if div else "내용 없음"

            # 공고기간 추출 (li.list-group-item 내 라벨+값 쌍)
            for li in soup.find_all('li', class_='list-group-item'):
                label_div = li.find('div', class_='col-sm-2')
                value_div = li.find('div', class_='list-group-item-text')
                if label_div and value_div:
                    label = label_div.get_text(strip=True)
                    if '공고일' in label or '접수기간' in label or '신청기간' in label:
                        period_text = value_div.get_text(strip=True)
                        if period_text:
                            item['신청기간'] = period_text
                            break
        except Exception as e:
            item['사업개요'] = "크롤링 실패"
        return item

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(_fetch, data_list))
    return results


def crawl_gwtp_list(end_page=3):
    """
    강원테크노파크(GWTP) 목록 크롤링 (Requests + Base64)
    URL: https://www.gwtp.or.kr/gwtp/bbsNew_list.php
    필터: 행 텍스트에 '모집중' 포함
    """
    print(f"\n[강원테크노파크] 1~{end_page}페이지(추정) 목록 크롤링 시작...")
    # Base params structure
    base_params_tmpl = "startPage={}&code=sub01b&table=cs_bbs_data_new&search_item=&search_order=&url=sub01b&keyvalue=sub01"
    
    results = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    # Iterate with step 10. Assuming end_page=3 means checking offsets 0, 10, 20.
    for page_offset in range(0, end_page * 10, 10):
        try:
            params_str = base_params_tmpl.format(page_offset)
            encoded_params = base64.b64encode(params_str.encode('utf-8')).decode('utf-8')
            url = f"https://www.gwtp.or.kr/gwtp/bbsNew_list.php?bbs_data={encoded_params}"
            
            response = GLOBAL_SESSION.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            rows = soup.select('table tbody tr')
            if not rows:
                print(f"  Offset {page_offset}: 게시물이 없습니다.")
                break
                
            print(f"  Offset {page_offset} 크롤링 중... ({len(rows)}개 항목 검사)")
            
            page_results = []
            
            for row in rows:
                try:
                    # 텍스트 전체에서 '모집중' 확인
                    if "모집중" not in row.get_text():
                        continue
                     
                    cols = row.find_all('td')
                    if not cols:
                        continue

                    # 제목 (보통 두 번째 <td>) -> <a> 태그 찾기
                    a_tag = row.find('a')
                    if not a_tag:
                        continue
                        
                    title = a_tag.text.strip()
                    link_suffix = a_tag.get('href')
                    if not link_suffix:
                        continue
                        
                    link = f"https://www.gwtp.or.kr/gwtp/{link_suffix}"
                    
                    # 날짜 search (YYYY-MM-DD pattern in row text)
                    date_text = ""
                    date_match = re.search(r'\d{4}-\d{2}-\d{2}', row.text)
                    if date_match:
                        date_text = date_match.group(0)
                    
                    # 중복 제거 (링크에 startPage가 포함되어 변하므로 제목+날짜로 비교)
                    if any(r['지원사업명'] == title and r['신청기간'] == date_text for r in results) or any(r['지원사업명'] == title and r['신청기간'] == date_text for r in page_results):
                        continue
                        
                    page_results.append({
                        '출처': '강원테크노파크',
                        '지원사업명': title,
                        '신청기간': date_text, # 등록일
                        '링크': link,
                        '사업개요': '' 
                    })
                    
                except Exception as e:
                    print(f"    [오류] 항목 파싱 실패: {e}")
            
            if not page_results:
                # 페이지에 '모집중'이 하나도 없거나 모두 중복이면 종료?
                # 아니면 다음 페이지에 있을 수 있으니 계속?
                pass
                
            results.extend(page_results)
            
        except Exception as e:
            print(f"  [오류] Offset {page_offset} 크롤링 중: {e}")
            
    print(f"[강원테크노파크] 총 {len(results)}개의 '모집중' 공고 추출 완료")
    return results

def crawl_gwtp_details(data_list):
    """
    강원테크노파크 상세 페이지 병렬 크롤링
    대상: td.img_td
    """
    print(f"[강원테크노파크] {len(data_list)}개 상세 페이지 병렬 크롤링 시작...")
    _hdrs = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

    def _fetch(item):
        try:
            r = GLOBAL_SESSION.get(item['\ub9c1\ud06c'], headers=_hdrs, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')
            td = soup.select_one('td.img_td')
            item['\uc0ac\uc5c5\uac1c\uc694'] = td.get_text(separator='\n', strip=True) if td else "\ub0b4\uc6a9 \uc5c6\uc74c"
        except Exception as e:
            item['\uc0ac\uc5c5\uac1c\uc694'] = "\ud06c\ub864\ub9c1 \uc2e4\ud328"
        return item

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(_fetch, data_list))
    return results


def crawl_cbtp_list(end_page=3):
    """
    충북테크노파크(CBTP) 목록 크롤링 (Requests + DESAdapter + EUC-KR)
    URL: https://www.cbtp.or.kr/index.php?control=bbs&lm_uid=387&board_id=saup_notice&page=1
    필터: 상태(IMG alt)에 '진행' 포함
    """
    print(f"\n[충북테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    
    # Page param: page=1, 2, ...
    base_url = "https://www.cbtp.or.kr/index.php?control=bbs&lm_uid=387&board_id=saup_notice&page={}"
    
    results = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    # Use DESAdapter for legacy SSL
    session = requests.Session()
    session.mount('https://', DESAdapter())

    for page in range(1, end_page + 1):
        try:
            url = base_url.format(page)
            response = session.get(url, headers=headers)
            response.encoding = 'EUC-KR' # Force encoding
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            rows = soup.select('table tbody tr')
            if not rows:
                print(f"  {page}페이지: 게시물이 없습니다.")
                break
                
            print(f"  {page}페이지 크롤링 중... ({len(rows)}개 항목 검사)")
            
            page_results = []
            
            for row in rows:
                try:
                    cols = row.find_all('td')
                    if len(cols) < 3: 
                        continue
                        
                    # Status check (Index 1) - Check IMG alt
                    status_td = cols[1]
                    status_text = status_td.get_text(strip=True)
                    if not status_text:
                        img = status_td.find('img')
                        if img:
                            status_text = img.get('alt', '').strip()
                    
                    if "진행" not in status_text:
                        continue
                        
                    # Title (Index 2)
                    title_td = cols[2]
                    title_link = title_td.find('a')
                    title = title_td.get_text(strip=True)
                    
                    link = ""
                    if title_link:
                        href = title_link.get('href')
                        if href:
                            # Relative path usually
                            link = f"https://www.cbtp.or.kr{href}" if href.startswith('/') else f"https://www.cbtp.or.kr/{href}"
                    
                    # Date (Last Index)
                    date_text = cols[-1].get_text(strip=True)
                    
                    # 중복 제거
                    if any(r['링크'] == link for r in results) or any(r['링크'] == link for r in page_results):
                        continue
                        
                    page_results.append({
                        '출처': '충북테크노파크',
                        '지원사업명': title,
                        '신청기간': date_text, 
                        '링크': link,
                        '사업개요': '' # 상세 내용 없음
                    })
                    
                except Exception as e:
                    print(f"    [오류] 항목 파싱 실패: {e}")
            
            if not page_results:
                pass
                
            results.extend(page_results)
            
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[충북테크노파크] 총 {len(results)}개의 '진행' 공고 추출 완료")
    return results


def crawl_cbtp_details(data_list):
    """
    충북테크노파크 상세 크롤링 (상세 내용 없음)
    - 목록에서 이미 필요한 정보를 수집했으므로 그대로 반환
    """
    return data_list



def crawl_jbtp_list(end_page=3):
    """
    전북테크노파크 (JBTP) 크롤링
    - URL: https://www.jbtp.or.kr/index.jbtp?menuCd=DOM_000000102001000000
    - 필터: '마감일' 행의 날짜 오른쪽에 '접수중' 표시
    - 내용: 상세 내용 없음
    """
    results = []
    base_url = "https://www.jbtp.or.kr"
    
    # SSL 경고 무시
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    print(f"[전북테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    
    for page in range(1, end_page + 1):
        try:
            print(f"  {page}페이지 크롤링 중...")
            # cpath param for pagination
            url = f"https://www.jbtp.or.kr/index.jbtp?menuCd=DOM_000000102001000000&cpath={page}"
            
            response = GLOBAL_SESSION.get(url, timeout=10)
            if response.status_code != 200:
                print(f"    [오류] 페이지 로드 실패: {response.status_code}")
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            rows = soup.select('table tbody tr')
            print(f"    {len(rows)}개 항목 검사...")
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) <= 3:
                    continue
                    
                # Col 3: Status
                status_td = cols[3]
                status_text = status_td.get_text(strip=True)
                
                if "접수중" not in status_text:
                    continue
                    
                # Col 1: Title & Link
                title_td = cols[1]
                title = title_td.get_text(strip=True)
                link_tag = title_td.find('a')
                link = ""
                if link_tag and 'href' in link_tag.attrs:
                    href = link_tag['href']
                    if href.startswith('/'):
                        link = base_url + href
                    else:
                        link = href
                
                # Col 2: Date
                date_td = cols[2]
                date_text = date_td.get_text(strip=True)
                
                # 중복 확인
                if any(r['링크'] == link for r in results):
                    continue
                    
                item = {
                    '출처': '전북테크노파크',
                    '지원사업명': title,
                    '신청기간': date_text,
                    '링크': link,
                    '사업개요': '' # 상세 내용 없음
                }
                results.append(item)
                
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[전북테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results


def crawl_jntp_list(end_page=3):
    """
    전남테크노파크 (JNTP) 목록 크롤링
    - URL: https://www.jntp.or.kr/base/apiAnnouncement/List?page={page}
    - 필터: 5번째 컬럼(상태)이 '접수중'
    """
    results = []
    base_url = "https://www.jntp.or.kr"
    
    # SSL 경고 무시
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    print(f"[전남테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    
    # 헤더 추가
    headers = {
         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for page in range(1, end_page + 1):
        try:
            print(f"  {page}페이지 크롤링 중...")
            url = f"https://www.jntp.or.kr/base/apiAnnouncement/List?page={page}"
            
            response = GLOBAL_SESSION.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"    [오류] 페이지 로드 실패: {response.status_code}")
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            rows = soup.select('table tbody tr')
            print(f"    {len(rows)}개 항목 검사...")
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) <= 5:
                    continue
                    
                # Col 5: Status
                status_td = cols[5]
                status_text = status_td.get_text(strip=True)
                
                if "접수중" not in status_text:
                    continue
                    
                # Col 1: Title & Link
                title_td = cols[1]
                title = title_td.get_text(strip=True)
                link_tag = title_td.find('a')
                link = ""
                if link_tag and 'href' in link_tag.attrs:
                    href = link_tag['href']
                    # Link might be full or relative
                    # Inspection showed: https://www.jntp.or.kr:443/base/apiAnnouncement/read?announcement=729...
                    if href.startswith('/'):
                        link = base_url + href
                    else:
                        link = href
                
                # Col 4: Date
                date_td = cols[4]
                date_text = date_td.get_text(strip=True).replace('\n', '').replace('\r', '').strip()
                
                # 중복 확인
                if any(r['링크'] == link for r in results):
                    continue
                    
                item = {
                    '출처': '전남테크노파크',
                    '지원사업명': title,
                    '신청기간': date_text,
                    '링크': link,
                    '사업개요': '' # 상세 크롤링에서 채움
                }
                results.append(item)
                
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[전남테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results

def crawl_jntp_details(data_list):
    """
    전남테크노파크 (JNTP) 상세 크롤링 (병렬)
    - '사업목적' 및 '사업내용' 추출하여 '사업개요'에 저장
    """
    print(f"[전남테크노파크] {len(data_list)}개 항목 상세 병렬 수집 시작...")
    _hdrs = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    def _fetch(item):
        try:
            r = GLOBAL_SESSION.get(item['링크'], headers=_hdrs, timeout=10)
            if r.status_code != 200:
                return item
            soup = BeautifulSoup(r.text, 'html.parser')
            overview_parts = []
            for li in soup.select('div.annou-content div.tbl-content ul li'):
                for span in li.find_all('span', class_='txt'):
                    label = span.get_text(strip=True)
                    if "사업목적" in label or "사업내용" in label:
                        parent = span.find_parent('div')
                        if parent:
                            con = parent.find('div', class_='con')
                            if con:
                                c = con.get_text(strip=True)
                                if c:
                                    overview_parts.append(f"[{label}] {c}")
            if overview_parts:
                text = " ".join(overview_parts)
                item['사업개요'] = text[:497] + "..." if len(text) > 500 else text
        except Exception as e:
            pass
        return item

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(_fetch, data_list))
    return results



def crawl_gbtp_list(end_page=3):
    """
    경북테크노파크 (GBTP) 목록 크롤링
    - URL: https://www.gbtp.or.kr/user/board.do?bbsId=BBSMSTR_000000000021&pageIndex={page}
    - 필터: 4번째 컬럼(접수상태)이 '접수중'
    - 내용: 상세 내용 없음 (사용자 요청)
    """
    results = []
    base_url = "https://www.gbtp.or.kr"
    
    # SSL 경고 무시
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    print(f"[경북테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for page in range(1, end_page + 1):
        try:
            print(f"  {page}페이지 크롤링 중...")
            url = f"https://www.gbtp.or.kr/user/board.do?bbsId=BBSMSTR_000000000021&pageIndex={page}"
            
            response = GLOBAL_SESSION.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"    [오류] 페이지 로드 실패: {response.status_code}")
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            rows = soup.select('table tbody tr')
            print(f"    {len(rows)}개 항목 검사...")
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) <= 4:
                    continue
                    
                # Col 4: Status (접수상태)
                status_td = cols[4]
                status_text = status_td.get_text(strip=True)
                
                if "접수중" not in status_text:
                    continue
                    
                # Col 1: Title & Link
                title_td = cols[1]
                title = title_td.get_text(strip=True)
                link_tag = title_td.find('a')
                
                link = ""
                # Extract nttId from onclick="javascript:fn_detail('10633','2')"
                if link_tag and 'onclick' in link_tag.attrs:
                    onclick = link_tag['onclick']
                    # Pattern: fn_detail('nttId','pageIndex')
                    match = re.search(r"fn_detail\('([^']+)'", onclick)
                    if match:
                        nttId = match.group(1)
                        link = f"https://www.gbtp.or.kr/user/board/view.do?bbsId=BBSMSTR_000000000021&nttId={nttId}&pageIndex={page}"
                    else:
                        link = url # Fallback
                
                # Col 3: Date (접수기간) e.g., 2025-11-25~2026-01-07
                date_td = cols[3]
                date_text = date_td.get_text(strip=True)
                
                # 중복 확인
                if any(r['링크'] == link for r in results):
                    continue
                    
                item = {
                    '출처': '경북테크노파크',
                    '지원사업명': title,
                    '신청기간': date_text,
                    '링크': link,
                    '사업개요': '' # 상세 내용 없음
                }
                results.append(item)
                
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[경북테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results


def crawl_ptp_list(end_page=3):
    """
    포항테크노파크 (PTP) 목록 크롤링
    - URL: https://www.ptp.or.kr/main/board/index.do?menu_idx=116&manage_idx=15&viewPage={page}
    - 필터: 3번째 컬럼(상태)이 '접수중'
    - 내용: 상세 내용 없음 (사용자 요청)
    """
    results = []
    base_url = "https://www.ptp.or.kr"
    
    # SSL 경고 무시
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    print(f"[포항테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for page in range(1, end_page + 1):
        try:
            print(f"  {page}페이지 크롤링 중...")
            url = f"https://www.ptp.or.kr/main/board/index.do?menu_idx=116&manage_idx=15&pageIndex={page}"
            
            response = GLOBAL_SESSION.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"    [오류] 페이지 로드 실패: {response.status_code}")
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            rows = soup.select('table tbody tr')
            print(f"    {len(rows)}개 항목 검사...")
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) <= 3:
                    continue
                    
                # Col 3: Status (접수상태/상태)
                status_td = cols[3]
                status_text = status_td.get_text(strip=True)
                
                if "접수중" not in status_text:
                    continue
                    
                # Col 2: Title & Link
                title_td = cols[2]
                title = title_td.get_text(strip=True)
                link_tag = title_td.find('a')
                
                link = ""
                # Extract board_idx from onclick="viewBoard(7585);"
                if link_tag and 'onclick' in link_tag.attrs:
                    onclick = link_tag['onclick']
                    # Pattern: viewBoard(1234) or viewBoard('1234')
                    match = re.search(r"viewBoard\(['\"]?(\d+)['\"]?\)", onclick)
                    if match:
                        board_idx = match.group(1)
                        # Construct link: https://www.ptp.or.kr/main/board/view.do?menu_idx=116&manage_idx=15&board_idx={id}
                        link = f"https://www.ptp.or.kr/main/board/view.do?menu_idx=116&manage_idx=15&board_idx={board_idx}"
                    else:
                        link = url
                else:
                     # Fallback if href exists and is not # or javascript
                     if link_tag and link_tag.get('href') and 'javascript' not in link_tag.get('href'):
                         href = link_tag.get('href')
                         if href.startswith('/'):
                             link = base_url + href
                         else:
                             link = href
                     else:
                         link = url

                
                # Col 4: Date (접수기간) e.g., ~ 2026-01-07 17시
                date_td = cols[4]
                date_text = date_td.get_text(strip=True)
                
                # 중복 확인
                if any(r['링크'] == link for r in results):
                    continue
                    
                item = {
                    '출처': '포항테크노파크',
                    '지원사업명': title,
                    '신청기간': date_text,
                    '링크': link,
                    '사업개요': '' # 상세 내용 없음
                }
                results.append(item)
                
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[포항테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results

def crawl_gntp_list(end_page=3):
    """
    경남테크노파크 (GNTP) 목록 크롤링
    - URL: https://www.gntp.or.kr/biz/apply
    - 필터: 접수상태 컬럼이 '접수중'
    - 내용: 상세 내용 없음 (사용자 요청)
    """
    results = []
    base_url = "https://www.gntp.or.kr"
    
    # SSL 경고 무시
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    print(f"[경남테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for page in range(1, end_page + 1):
        try:
            print(f"  {page}페이지 크롤링 중...")
            # 페이지네이션은 JavaScript 기반이지만, 첫 페이지 데이터는 정적 HTML에 포함됨
            # 추가 페이지는 API 호출 또는 Selenium 필요할 수 있음
            url = f"https://www.gntp.or.kr/biz/apply?page={page}"
            
            response = GLOBAL_SESSION.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"    [오류] 페이지 로드 실패: {response.status_code}")
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 데스크탑 테이블 찾기: div.de-news table
            table = soup.select_one('div.de-news table')
            if not table:
                print(f"    테이블을 찾을 수 없습니다.")
                continue
            
            # tbody#gridData 내의 tr.table-contents 행들
            rows = table.select('tbody#gridData tr.table-contents')
            print(f"    {len(rows)}개 항목 검사...")
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 8:
                    continue
                    
                # Col 7: Status (접수상태) - '접수중' 필터
                status_td = cols[7]
                status_text = status_td.get_text(strip=True)
                
                if "접수중" not in status_text:
                    continue
                    
                # Col 4: Title & Link (공고명)
                title_td = cols[4]
                title = title_td.get_text(strip=True)
                link_tag = title_td.find('a')
                
                link = ""
                # onclick="goPage('S', null, '/biz/applyInfo/3572')"에서 경로 추출
                if link_tag and 'onclick' in link_tag.attrs:
                    onclick = link_tag['onclick']
                    match = re.search(r"goPage\([^,]+,\s*[^,]+,\s*'([^']+)'\)", onclick)
                    if match:
                        path = match.group(1)
                        link = f"{base_url}{path}"
                    else:
                        link = base_url + "/biz/apply"
                else:
                    link = base_url + "/biz/apply"
                
                # Col 6: 접수마감 -> 신청기간으로 사용
                deadline_td = cols[6]
                deadline_text = deadline_td.get_text(strip=True).replace('\n', ' ').strip()
                
                # 중복 확인
                if any(r['링크'] == link for r in results):
                    continue
                    
                item = {
                    '출처': '경남테크노파크',
                    '지원사업명': title,
                    '신청기간': deadline_text,
                    '링크': link,
                    '사업개요': ''  # 상세 내용 없음
                }
                results.append(item)
                
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[경남테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results

def crawl_jtp_list(driver=None, end_page=1):
    """
    제주테크노파크 (JTP) 목록 크롤링 (Selenium - Vue.js 동적 렌더링)
    - URL: https://www.jejutp.or.kr/board/business?keyword=&pageNumber=0&size=30&cate=
    - 필터: 접수여부 컬럼이 '신청가능'
    - 내용: 상세 내용 없음 (사용자 요청)
    - 주의: pageNumber=0이 1페이지, Vue.js 동적 렌더링 페이지
    """
    results = []
    base_url = "https://www.jejutp.or.kr"
    own_driver = False
    
    print(f"[제주테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    
    try:
        if driver is None:
            driver = setup_driver()
            own_driver = True
        
        for page in range(0, end_page):  # pageNumber=0이 1페이지
            try:
                print(f"  {page+1}페이지 크롤링 중...")
                url = f"https://www.jejutp.or.kr/board/business?keyword=&pageNumber={page}&size=30&cate="
                
                driver.get(url)
                time.sleep(1.5)  # Vue.js 렌더링 대기
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # 데스크탑 테이블: div.board-item-container.d-none.d-md-block > ul > li.board-item
                container = soup.select_one('div.board-item-container.d-none.d-md-block ul')
                if not container:
                    print(f"    테이블을 찾을 수 없습니다.")
                    continue
                
                # li.board-item 행들 (헤더 제외 - board-item-header 클래스 없는 것만)
                rows = container.select('li.board-item:not(.board-item-header)')
                print(f"    {len(rows)}개 항목 검사...")
                
                for row in rows:
                    # 내부 a 태그 찾기
                    link_tag = row.find('a')
                    if not link_tag:
                        continue
                    
                    # 접수여부 체크 - 텍스트에서 '신청가능' 포함 여부
                    row_text = row.get_text()
                    if "신청가능" not in row_text:
                        continue
                        
                    # 공고명 (w-md-34 클래스)
                    title_span = link_tag.select_one('span.w-md-34')
                    if not title_span:
                        # Fallback: w-100 w-md-34
                        title_span = link_tag.select_one('span[class*="w-md-34"]')
                    if not title_span:
                        continue
                    title = title_span.get_text(strip=True)
                    
                    # 링크
                    href = link_tag.get('href', '')
                    if href:
                        link = base_url + href
                    else:
                        link = base_url + "/board/business"
                    
                    # 접수종료일 (w-15 클래스) -> 신청기간으로 사용
                    deadline_span = link_tag.select_one('span.w-15')
                    deadline_text = deadline_span.get_text(strip=True) if deadline_span else ""
                    
                    # 중복 확인
                    if any(r['링크'] == link for r in results):
                        continue
                        
                    item = {
                        '출처': '제주테크노파크',
                        '지원사업명': title,
                        '신청기간': deadline_text,
                        '링크': link,
                        '사업개요': ''  # 상세 내용 없음
                    }
                    results.append(item)
                    
            except Exception as e:
                print(f"  [오류] {page+1}페이지 크롤링 중: {e}")
                
    except Exception as e:
        print(f"[오류] Selenium 드라이버 초기화 실패: {e}")
    finally:
        if own_driver and driver:
            try:
                driver.quit()
            except:
                pass
            
    print(f"[제주테크노파크] 총 {len(results)}개의 '신청가능' 공고 추출 완료")
    return results

def crawl_ctp_list(end_page=3):
    """
    충남테크노파크(CTP) 목록 크롤링 (Requests)
    URL: https://www.ctp.or.kr/business/data.do?&pn=1
    필터: '마감일' 또는 행에 '접수중' 포함 여부
    """
    print(f"\n[충남테크노파크] 1~{end_page}페이지 목록 크롤링 시작...")
    
    # Page param: pn=1, 2, ...
    base_url = "https://www.ctp.or.kr/business/data.do?&pn={}"
    
    results = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for page in range(1, end_page + 1):
        try:
            url = base_url.format(page)
            # CTP는 일반 requests로 가능 (SSL verify=False)
            response = GLOBAL_SESSION.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            rows = soup.select('table tbody tr')
            if not rows:
                print(f"  {page}페이지: 게시물이 없습니다.")
                break
                
            print(f"  {page}페이지 크롤링 중... ({len(rows)}개 항목 검사)")
            
            page_results = []
            
            for row in rows:
                try:
                    cols = row.find_all('td')
                    if not cols:
                        continue
                        
                    row_text = row.get_text(strip=True)
                    
                    # '접수중' 필터링 (간편하게 전체 텍스트에서 확인)
                    # 이미지에는 '접수중'이 파란색 뱃지로 텍스트로 존재함.
                    if "접수중" not in row_text:
                        continue
                        
                    # Title (Index 1 usually, cols[1])
                    if len(cols) > 1:
                        title_td = cols[1]
                        a_tag = title_td.find('a')
                        if not a_tag:
                            continue
                            
                        title = a_tag.get_text(strip=True)
                        href = a_tag.get('href')
                        
                        link = ""
                        if href:
                            # href="datadetail.do?seq=..." -> Relative
                            link = f"https://www.ctp.or.kr/business/{href}"
                        
                        # Date (Deadline) - Index 2
                        # Format: "2025-12-15 18:00접수중" 
                        # We extract YYYY-MM-DD
                        date_text = ""
                        if len(cols) > 2:
                             deadline_text = cols[2].get_text(strip=True)
                             date_match = re.search(r'\d{4}-\d{2}-\d{2}', deadline_text)
                             if date_match:
                                 date_text = date_match.group(0)
                        
                        # 중복 제거
                        if any(r['링크'] == link for r in results) or any(r['링크'] == link for r in page_results):
                            continue
                            
                        page_results.append({
                            '출처': '충남테크노파크',
                            '지원사업명': title,
                            '신청기간': date_text, 
                            '링크': link,
                            '사업개요': '' # 상세 내용 없음
                        })
                    
                except Exception as e:
                    print(f"    [오류] 항목 파싱 실패: {e}")
            
            results.extend(page_results)
            
        except Exception as e:
            print(f"  [오류] {page}페이지 크롤링 중: {e}")
            
    print(f"[충남테크노파크] 총 {len(results)}개의 '접수중' 공고 추출 완료")
    return results

def crawl_ctp_details(data_list):
    # 상세 내용 수집 없음
    return data_list


# ============================================================
# 소상공인24 (sbiz24.kr) 크롤링
# ============================================================

# 스레드별 드라이버 관리 (병렬 상세 크롤링용)
_sbiz24_thread_local = threading.local()

def _get_sbiz24_thread_driver():
    """스레드별로 독립적인 WebDriver 인스턴스 반환"""
    if not hasattr(_sbiz24_thread_local, 'driver') or _sbiz24_thread_local.driver is None:
        _sbiz24_thread_local.driver = setup_driver()
    return _sbiz24_thread_local.driver

def _close_sbiz24_thread_driver():
    """스레드의 WebDriver 종료"""
    if hasattr(_sbiz24_thread_local, 'driver') and _sbiz24_thread_local.driver:
        try:
            _sbiz24_thread_local.driver.quit()
        except:
            pass
        _sbiz24_thread_local.driver = None

def crawl_sbiz24_list(end_page=SBIZ24_END_PAGE):
    """
    소상공인24(sbiz24.kr) 목록 크롤링
    URL: https://www.sbiz24.kr/#/combinePbancList?page={page}
    필터: 상태 == '신청가능'
    출력: 공통 스키마 (지원사업명, 신청기간, 사업개요, 링크, 출처)
    """
    print(f"\n[소상공인24] 1~{end_page}페이지 목록 크롤링 시작 (SPA/Vue.js)...")
    driver = setup_driver()
    all_data = []
    seen_links = set()  # 중복 방지: 이미 수집한 링크 추적

    try:
        for page in range(1, end_page + 1):
            url = f"https://www.sbiz24.kr/#/combinePbancList?page={page}"
            print(f"  [소상공인24] 페이지 {page} 크롤링 중: {url}")

            try:
                driver.get(url)
                WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )
                time.sleep(0.3)

                tables = driver.find_elements(By.TAG_NAME, "table")
                if not tables:
                    print(f"  페이지 {page}: 테이블 없음")
                    break

                rows = tables[0].find_elements(By.TAG_NAME, "tr")
                page_items = 0
                page_new_items = 0

                for row in rows:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) >= 7:
                        status = cols[6].text.strip()
                        if status == "신청가능":
                            title_col = cols[2]
                            title = title_col.text.strip()
                            try:
                                link_el = title_col.find_element(By.TAG_NAME, "a")
                                link = link_el.get_attribute("href") or "N/A"
                            except:
                                link = "N/A"
                            period = cols[4].text.strip()
                            page_items += 1

                            # 이미 수집한 링크면 건너뜀
                            if link in seen_links:
                                continue
                            seen_links.add(link)
                            page_new_items += 1

                            all_data.append({
                                '출처': '소상공인24',
                                '지원사업명': title,
                                '신청기간': period,
                                '링크': link,
                                '사업개요': ''  # 상세 크롤링 전 빈 값
                            })

                print(f"  페이지 {page}: {page_items}개 '신청가능' 항목 중 {page_new_items}개 신규 수집")

                # 페이지에서 신규 항목이 하나도 없으면 사이트가 더 이상 새 페이지를 제공하지 않는 것 → 조기 종료
                if page > 1 and page_new_items == 0:
                    print(f"  [소상공인24] 페이지 {page}에서 신규 항목 없음 → 크롤링 종료")
                    break

            except Exception as e:
                print(f"  [오류] 페이지 {page} 처리 중: {e}")

    finally:
        try:
            driver.quit()
        except:
            pass

    print(f"[소상공인24] 목록 크롤링 완료. 총 {len(all_data)}개 항목 (고유)")
    return all_data

def _crawl_single_sbiz24_detail(item, index, total):
    """단일 소상공인24 상세 페이지 크롤링 (병렬 처리용)"""
    link = item.get('링크')
    if not link or link == "N/A":
        item['사업개요'] = "링크 없음"
        return item

    driver = _get_sbiz24_thread_driver()

    try:
        driver.get(link)
        time.sleep(0.5)

        content = ""

        # Case 1: .subcon_w100 (내부 공고)
        try:
            content = driver.find_element(By.CLASS_NAME, "subcon_w100").text.strip()
        except:
            pass

        # Case 2: .f_bizOutlCn .form-wrap (외부 공고)
        if not content:
            try:
                container = driver.find_element(By.CLASS_NAME, "f_bizOutlCn")
                content = container.find_element(By.CLASS_NAME, "form-wrap").text.strip()
            except:
                pass

        # Case 3: '공고내용' 라벨 기반 (Fallback)
        if not content:
            try:
                label = driver.find_element(By.XPATH, "//label[contains(text(), '공고내용')]")
                content = label.find_element(By.XPATH, "following-sibling::div").text.strip()
            except:
                pass

        item['사업개요'] = content if content else "내용 추출 실패"

    except Exception as e:
        if "invalid session id" in str(e) or "session deleted" in str(e):
            _close_sbiz24_thread_driver()
            _get_sbiz24_thread_driver()
        item['사업개요'] = "크롤링 오류"

    time.sleep(0.3)
    return item

def crawl_sbiz24_details(data):
    """소상공인24 상세 내용 병렬 크롤링 (4 workers)"""
    print(f"\n[소상공인24] 상세 크롤링 시작 (총 {len(data)}개 항목, 4 workers)...")

    MAX_WORKERS = 4
    completed = 0
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_crawl_single_sbiz24_detail, item, i, len(data)): i
            for i, item in enumerate(data)
        }
        for future in as_completed(futures):
            idx = futures[future]
            completed += 1
            try:
                result = future.result()
                results.append((idx, result))
                if completed % 10 == 0 or completed == len(data):
                    print(f"  [소상공인24] 진행: {completed}/{len(data)} ({100*completed//len(data)}%)")
            except Exception as e:
                print(f"  [소상공인24] 작업 {idx} 실패: {e}")
                data[idx]['사업개요'] = "크롤링 오류"
                results.append((idx, data[idx]))

    results.sort(key=lambda x: x[0])
    print("[소상공인24] 상세 크롤링 완료")
    return [r[1] for r in results]


def main():
    print("=" * 60)
    print("통합 크롤링 프로세스 시작 (jiwon + sbiz24)")
    print("=" * 60)
    final_results = []
    driver = None


    # 1. 기업마당 (BizInfo)
    try:
        # driver는 setup_driver()에서 한 번만 초기화하고 재사용
        driver = setup_driver() # Initial driver setup
        bizinfo_list = crawl_bizinfo_list(driver) # Pass driver to list crawler
        if bizinfo_list:
            # 상세 페이지 크롤링 (Selenium 사용) - driver 반환 받음
            bizinfo_details, driver = crawl_bizinfo_details(driver, bizinfo_list) # Pass driver first
            # 출처 추가
            for item in bizinfo_details: # Ensure '출처' is added to detailed items
                item['출처'] = '비즈인포'
            final_results.extend(bizinfo_details)
    except Exception as e:
        # print(f"[오류] 기업마당 크롤링 실패: {e}")
        # If BizInfo fails and driver might be broken, try to re-initialize for next steps
        try:
            if driver:
                driver.quit()
            driver = setup_driver()
        except:
            driver = None

    # 2. 부산테크노파크 (BTP)
    try:
        # driver 상태 확인 및 재사용
        if driver is None:
             driver = setup_driver()
    
        btp_list = crawl_btp_list(driver)
        if btp_list:
            btp_details = crawl_btp_details(driver, btp_list)
            final_results.extend(btp_details)
    except Exception as e:
        print(f"[오류] 부산테크노파크 크롤링 실패: {e}")
        try:
            if driver:
                driver.quit()
            driver = setup_driver()
        except:
            driver = None

    # 3. 대구테크노파크 (DGTP)
    try:
        # driver 상태 확인 및 재사용
        if driver is None:
             driver = setup_driver()
             
        # DGTP 리스트 크롤링 (requests 사용)
        dgtp_list = crawl_dgtp_list(end_page=END_PAGE) # DGTP list uses requests, no driver needed here
        
        # DGTP 상세 크롤링 (Selenium 사용)
        if dgtp_list:
             dgtp_details, driver = crawl_dgtp_details(dgtp_list, driver) # DGTP details uses Selenium, needs driver
             final_results.extend(dgtp_details)
            
    except Exception as e:
         print(f"[오류] 대구테크노파크 크롤링 실패: {e}")
         try:
            if driver:
                driver.quit()
            driver = setup_driver()
         except:
            driver = None

    # 4. 인천테크노파크 (ITP)
    try:
        # driver 상태 확인 및 재사용
        if driver is None:
             driver = setup_driver()
             
        itp_results = crawl_itp_list(driver)
        if itp_results:
             final_results.extend(itp_results)
             
    except Exception as e:
        print(f"[오류] 인천테크노파크 크롤링 실패: {e}")

    # 5. 광주테크노파크 (GJTP)
    try:
        gjtp_list = crawl_gjtp_list(end_page=END_PAGE)
        if gjtp_list:
            final_results.extend(crawl_gjtp_details(gjtp_list))
    except Exception as e:
        print(f"[오류] 광주테크노파크 크롤링 실패: {e}")

    # ── Phase 2: requests 기반 크롤러 병렬 실행 (독립적 13개 사이트) ──
    def _run_requests_crawler(name, fn_list, fn_detail=None, end_page=END_PAGE):
        """requests 기반 크롤러 실행 래퍼 (list + optional detail)"""
        try:
            items = fn_list(end_page=end_page) if end_page else fn_list()
            if items and fn_detail:
                items = fn_detail(items)
            return items or []
        except Exception as e:
            print(f"[오류] {name} 크롤링 실패: {e}")
            return []

    print("\n[병렬] requests 기반 크롤러 동시 실행 시작...")
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures_map = {
            ex.submit(_run_requests_crawler, "대전테크노파크",   crawl_djtp_list, None,              END_PAGE): "대전TP",
            ex.submit(_run_requests_crawler, "세종테크노파크",   crawl_sjtp_list, None,              END_PAGE): "세종TP",
            ex.submit(_run_requests_crawler, "경기테크노파크",   crawl_gtp_list,  None,              END_PAGE): "경기TP",
            ex.submit(_run_requests_crawler, "경기대진테크노파크", crawl_gdtp_list, crawl_gdtp_details, END_PAGE): "경기대진TP",
            ex.submit(_run_requests_crawler, "강원테크노파크",   crawl_gwtp_list, crawl_gwtp_details, END_PAGE): "강원TP",
            ex.submit(_run_requests_crawler, "충북테크노파크",   crawl_cbtp_list, crawl_cbtp_details, END_PAGE): "충북TP",
            ex.submit(_run_requests_crawler, "충남테크노파크",   crawl_ctp_list,  crawl_ctp_details,  END_PAGE): "충남TP",
            ex.submit(_run_requests_crawler, "전북테크노파크",   crawl_jbtp_list, None,              END_PAGE): "전북TP",
            ex.submit(_run_requests_crawler, "전남테크노파크",   crawl_jntp_list, crawl_jntp_details, END_PAGE): "전남TP",
            ex.submit(_run_requests_crawler, "경북테크노파크",   crawl_gbtp_list, None,              END_PAGE): "경북TP",
            ex.submit(_run_requests_crawler, "포항테크노파크",   crawl_ptp_list,  None,              END_PAGE): "포항TP",
            ex.submit(_run_requests_crawler, "경남테크노파크",   crawl_gntp_list, None,              END_PAGE): "경남TP",
        }
        for future in as_completed(futures_map):
            label = futures_map[future]
            try:
                items = future.result()
                if items:
                    final_results.extend(items)
                    print(f"  [완료] {label}: {len(items)}개 수집")
            except Exception as e:
                print(f"  [오류] {label}: {e}")
    print("[병렬] requests 기반 크롤러 모두 완료")



    # 7. 울산테크노파크 (UTP)
    try:
        if driver is None:
             driver = setup_driver()
             
        utp_list = crawl_utp_list(driver, end_page=END_PAGE)
        if utp_list:
            utp_details = crawl_utp_details(utp_list, driver)
            final_results.extend(utp_details)
    except Exception as e:
        print(f"[오류] 울산테크노파크 크롤링 실패: {e}")
    


    # 19. 제주테크노파크 (JTP)
    try:
        # Selenium 사용 (Vue.js 동적 렌더링), 1페이지만 크롤링
        jtp_list = crawl_jtp_list(driver=driver, end_page=1)
        if jtp_list:
             # 상세 내용 없음
             final_results.extend(jtp_list)
    except Exception as e:
        print(f"[오류] 제주테크노파크 크롤링 실패: {e}")

    # 드라이버 종료 (jiwon 계열 공통 드라이버)
    try:
        if driver:
            driver.quit()
    except:
        pass

    # 20. 소상공인24 (sbiz24.kr) - 별도 드라이버 사용
    try:
        sbiz24_list = crawl_sbiz24_list(end_page=SBIZ24_END_PAGE)
        if sbiz24_list:
            sbiz24_details = crawl_sbiz24_details(sbiz24_list)
            final_results.extend(sbiz24_details)
    except Exception as e:
        print(f"[오류] 소상공인24 크롤링 실패: {e}")

    # ── 결과 저장 ──────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"통합 크롤링 완료. 총 {len(final_results)}개 항목 수집")
    print("=" * 60)

    if not final_results:
        print("수집된 데이터가 없습니다.")
        return

    # 웹 뷰어 data.json 갱신
    saved_count = save_to_web_json(final_results)

    # 출처별 통계 집계 및 로그 기록
    source_counts = {}
    for item in final_results:
        src = item.get('출처', '비즈인포')
        source_counts[src] = source_counts.get(src, 0) + 1
    write_crawl_log(saved_count, source_counts)

    print(f"\n✅ 웹 뷰어 데이터 갱신 완료 (총 {saved_count}개 항목)")

    # ── Git 자동 배포 ───────────────────────────────────────
    print("\n[GitHub] 최신 크롤링 데이터 자동 배포 시작...")
    try:
        web_dir = os.path.join(os.path.dirname(__file__), 'biz_support_web')
        commit_msg = f"auto: 크롤링 데이터 업데이트 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
        
        # 1. git add
        subprocess.run(["git", "add", "data.json", "index.html"], cwd=web_dir, check=True, capture_output=True)
        
        # 2. git commit (변경 사항이 없을 수도 있으므로 예외 처리)
        commit_result = subprocess.run(["git", "commit", "-m", commit_msg], cwd=web_dir, capture_output=True, text=True)
        if "nothing to commit" in commit_result.stdout:
            print("[GitHub] 변경 사항이 없어 커밋/푸시를 생략합니다.")
        else:
            # 3. git push
            subprocess.run(["git", "push", "origin", "main"], cwd=web_dir, check=True, capture_output=True)
            print("[GitHub] 성공적으로 배포(git push)를 완료했습니다! 🚀")
            
    except subprocess.CalledProcessError as e:
        print(f"[오류] 자동 배포 중 문제가 발생했습니다: {e.stderr.decode('utf-8', 'ignore')}")
    except Exception as e:
        print(f"[오류] 배포 스크립트 실행 실패: {e}")


if __name__ == "__main__":
    main()
