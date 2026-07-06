import requests
import urllib3
import base64
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import math
import json
import re
import io
import zipfile
import zlib
import struct
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET
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
PHASE2_WORKERS = 12  # Phase 2 requests 병렬 worker (Scrapling 도입 후 8 → 12 상향)

# ── 핵심정보(첨부 공고문 구조화 추출) 설정 ─────────────────────────
# 첨부 다운로드는 비즈인포 단일 도메인이라 PHASE2_WORKERS(12) 투입 금지 — 전용 4.
KEYINFO_VERSION = 1            # 추출 스키마 버전 — 로직 개선 시 bump → 통제된 재백필
EXTRACT_CAP = int(os.environ.get('EXTRACT_CAP', '200'))        # run당 첨부 보강 상한
EXTRACT_DEADLINE_SEC = int(os.environ.get('EXTRACT_DEADLINE_SEC', '600'))
EXTRACT_WORKERS = 4

# ────────────────────────────────────────────────────────────────────
# Scrapling Fetcher (curl_cffi 기반 TLS fingerprint spoofing + HTTP/2)
# ────────────────────────────────────────────────────────────────────
# 도입 배경: 한국 공공기관 사이트 일부가 기본 requests UA를 차단하거나 HTTP/1.1
# 만 응답 → 차단/지연이 누적. Scrapling Fetcher는 실제 Chrome의 TLS 지문을
# 흉내내고 HTTP/2를 지원해서 차단률·latency가 동시에 개선된다 (BS4 대비 파서
# 속도는 부수적 이득).
#
# 운영 안전장치:
#   1) Scrapling import 실패해도 requests fallback으로 100% 기존 동작 유지.
#   2) DESAdapter(레거시 SSL) 등 특수 session이 명시되면 Scrapling 건너뜀.
#   3) Fetcher 호출이 예외/빈 본문을 던지면 조용히 requests로 폴백.
# ────────────────────────────────────────────────────────────────────
_SCRAPLING_FETCHER = None
try:
    from scrapling.fetchers import Fetcher as _ScraplingFetcher  # type: ignore
    _SCRAPLING_FETCHER = _ScraplingFetcher
    print("[Scrapling] Fetcher 활성화 (curl_cffi + TLS spoofing)")
except Exception as _scrapling_import_err:
    print(f"[Scrapling] 미설치 — 기존 requests로 동작합니다 ({_scrapling_import_err})")


def _merge_url_params(url, params):
    """URL 쿼리스트링에 dict params를 병합해 새 URL 반환."""
    if not params:
        return url
    from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl
    parsed = urlparse(url)
    merged = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for k, v in params.items():
        merged[str(k)] = str(v)
    return urlunparse(parsed._replace(query=urlencode(merged)))


def _maybe_relay(final_url, headers):
    """비즈인포 릴레이 활성 시 www.bizinfo.go.kr URL을 릴레이 URL로 투명 재작성.

    _BIZINFO_STATE['relay']가 True(직접 접속 실패 후 폴백 발동)이고 대상 호스트가
    www.bizinfo.go.kr일 때만 URL을 BIZINFO_RELAY_BASE로 재작성하고 X-Relay-Key
    헤더를 병합한다. 이 한 지점으로 엑셀·상세페이지 enrich·첨부 다운로드가 전부
    릴레이를 타므로 기존 파서·필터·보강 코드를 무변경 재사용한다.

    env(BIZINFO_RELAY_BASE) 미설정 로컬 환경에서는 relay 플래그가 켜질 수 없어
    완전한 no-op — 기존 동작과 동일. (상수들은 모듈 하단에 정의되지만 호출 시점
    조회라 전방 참조 문제 없음.)
    """
    try:
        if not (_BIZINFO_STATE.get('relay') and BIZINFO_RELAY_BASE):
            return final_url, headers
        parsed = urlparse(final_url)
        if parsed.netloc != 'www.bizinfo.go.kr':
            return final_url, headers
        new_url = BIZINFO_RELAY_BASE + parsed.path + (('?' + parsed.query) if parsed.query else '')
        merged = dict(headers or {})
        merged['X-Relay-Key'] = BIZINFO_RELAY_KEY
        return new_url, merged
    except Exception:
        return final_url, headers


def fetch_html(url, *, session=None, timeout=20, params=None, headers=None,
               encoding=None, allow_scrapling=True):
    """
    공용 HTML fetcher. Scrapling Fetcher → requests 순서로 시도하고 raw text(str) 반환.

    Args:
      url: 요청 URL
      session: 명시되면 Scrapling을 건너뛰고 이 session으로만 요청 (예: CBTP DESAdapter)
      timeout: 초 단위
      params: dict이면 URL에 자동 병합 (양쪽 백엔드 일관 동작 보장)
      headers: 추가 헤더 (UA 등)
      encoding: 'EUC-KR' 등 강제 인코딩. None이면 자동 감지.
      allow_scrapling: False로 호출하면 무조건 requests 경로 사용 (디버깅용).

    Returns:
      HTML 문자열. 두 경로 모두 실패하면 마지막 예외를 그대로 raise.
    """
    final_url = _merge_url_params(url, params)
    final_url, headers = _maybe_relay(final_url, headers)  # 비즈인포 릴레이 (비활성 시 no-op)

    # 1) Scrapling Fetcher 시도 — 단, 명시 session이 없을 때만
    if session is None and allow_scrapling and _SCRAPLING_FETCHER is not None:
        try:
            # verify=False: requests 경로(GLOBAL_SESSION.verify=False)와 동일 정책.
            # 한국 공공기관 다수가 중간 CA를 누락한 불완전 인증서 체인을 제공 →
            # Scrapling(curl_cffi) 기본 verify=True면 curl(60) "unable to get local
            # issuer certificate"로 실패하고 내부 재시도·requests 폴백 지연이 누적된다.
            # 검증을 끄면 Scrapling 경로가 1차에 성공 → 수집 성공률·속도 동시 개선.
            kwargs = {'timeout': timeout, 'verify': False}
            try:
                # 지원하지 않는 버전이면 TypeError로 빠지고 fallback
                kwargs['impersonate'] = 'chrome120'
            except Exception:
                pass
            if headers:
                kwargs['headers'] = headers
            page = _SCRAPLING_FETCHER.get(final_url, **kwargs)

            # 응답 본문 추출 — Scrapling 버전에 따라 attribute가 다르다
            body = getattr(page, 'body', None)
            if isinstance(body, (bytes, bytearray)):
                if encoding:
                    return body.decode(encoding, errors='replace')
                try:
                    return body.decode('utf-8')
                except UnicodeDecodeError:
                    return body.decode('cp949', errors='replace')
            for attr in ('text', 'html_content'):
                t = getattr(page, attr, None)
                if isinstance(t, str) and len(t) > 50:
                    return t
            s = str(page)
            if s and len(s) > 50:
                return s
        except Exception:
            # 조용히 fallback — 운영 중단 방지
            pass

    # 2) Fallback: 전달된 session 또는 GLOBAL_SESSION
    sess = session or GLOBAL_SESSION
    r = sess.get(final_url, headers=headers, timeout=timeout, verify=False)
    r.raise_for_status()
    if encoding:
        r.encoding = encoding
    elif not r.encoding or r.encoding.lower() == 'iso-8859-1':
        r.encoding = r.apparent_encoding or r.encoding
    return r.text


def fetch_bytes(url, *, params=None, timeout=30, headers=None):
    """바이너리 콘텐츠(엑셀/PDF 등)를 받기 위한 fetcher.

    fetch_html은 text(str)를 반환하므로 xlsx 같은 바이너리에는 부적합.
    Scrapling Fetcher(TLS 스푸핑) 우선 시도 후 GLOBAL_SESSION으로 fallback.
    두 경로 모두 bytes를 반환한다.
    """
    final_url = _merge_url_params(url, params)
    final_url, headers = _maybe_relay(final_url, headers)  # 비즈인포 릴레이 (비활성 시 no-op)
    if _SCRAPLING_FETCHER is not None:
        try:
            # verify=False: fetch_html과 동일 — 공공기관 불완전 인증서 체인 대응.
            # 비즈인포 엑셀 등 바이너리 fetch가 curl(60) SSL 실패로 빈 결과를 반환하던
            # 문제를 막는다(Scrapling TLS 스푸핑 경로를 실제로 활용 → 차단·타임아웃 완화).
            page = _SCRAPLING_FETCHER.get(final_url, timeout=timeout, verify=False,
                                          **({'headers': headers} if headers else {}))
            body = getattr(page, 'body', None)
            if isinstance(body, (bytes, bytearray)) and len(body) > 1000:
                return bytes(body)
        except Exception:
            pass  # 조용히 fallback
    r = GLOBAL_SESSION.get(final_url, headers=headers, timeout=timeout, verify=False)
    r.raise_for_status()
    return r.content


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

# ── 비즈인포(기업마당) 신 시스템(selectSIIA200) 대응 설정 ─────────────────
# 2026년 기업마당이 구 게시판(/web/lay1/bbs/S1T122C128)에서 신 시스템으로 전환.
# 구 URL은 302 리다이렉트되고, '최신 등록순 150건'만 긁던 기존 방식은 K-뷰티론
# 정책자금처럼 '예산 소진시까지' 오래 열려있는 인기 공고를 통째로 누락시켰다.
# → 엑셀 일괄 다운로드(selectSIIA200ExcelDownload.do)로 전체 목록을 받아
#   '현재 신청 가능 + 직접지원성 핵심 분야'만 필터링해 누락을 근본적으로 없앤다.
BIZINFO_EXCEL_URL = "https://www.bizinfo.go.kr/sii/siia/selectSIIA200ExcelDownload.do"
# 직접지원성 핵심 분야(~700건). 노이즈 비중 큰 경영/인력/내수/기타는 제외.
# 수집 범위를 넓히려면 '경영','인력','내수' 등을 추가하면 됨.
BIZINFO_INCLUDE_FIELDS = {'금융', '기술', '수출', '창업'}
# 단순 안내/결과 발표성 공고 제외 (사업 기회가 아님)
BIZINFO_NOISE_RE = re.compile(
    r'선정\s*결과|결과\s*발표|모집\s*결과|평가\s*결과|설명회|간담회|안내문|'
    r'연기\s*공고|변경\s*공고|재공고\s*안내'
)

# ── P0: 비즈인포 릴레이/기업마당 JSON API 폴백 설정 ──────────────────────
# GitHub Actions(Azure) 대역이 비즈인포 방화벽에 표적 차단(TCP SYN 드롭, 14일+)
# → Cloudflare Pages 릴레이(seoryu)로 우회하는 4단 폴백 사다리:
#   [엑셀 직접 → 엑셀 릴레이 → 기업마당 JSON API(릴레이) → 이전 데이터 보존]
# env 미설정(로컬 기본) 시 릴레이·API 경로 전부 비활성 → 현행과 동일 동작.
BIZINFO_RELAY_BASE = os.environ.get('BIZINFO_RELAY_BASE', '').rstrip('/')
BIZINFO_RELAY_KEY = os.environ.get('BIZINFO_RELAY_KEY', '')
BIZINFO_API_KEY = os.environ.get('BIZINFO_API_KEY', '')  # 기업마당 crtfcKey
BIZINFO_API_URL = 'https://www.bizinfo.go.kr/uss/rss/bizinfoApi.do'
# 기업마당 지원분야 대분류 코드 (JSON API searchLclasId)
_BIZINFO_LCLAS = {'01': '금융', '02': '기술', '03': '인력', '04': '수출',
                  '05': '내수', '06': '창업', '07': '경영', '09': '기타'}
# 모듈 상태: relay=투명 릴레이 발동 여부, mode ∈ excel|api|preserved|None
_BIZINFO_STATE = {'relay': False, 'mode': None}

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


# ────────────────────────────────────────────────────────────────────
# 상세 본문/첨부파일 추출 헬퍼
#
# 한국 공공기관 사이트의 표준 패턴: 웹 상세 페이지에는 메타데이터(제목·접수기간·
# 담당자)만 있고, 진짜 사업 내용은 첨부 공고문(HWP/HWPX/PDF) 안에 있다.
# 따라서 (1) HTML에서 키워드 영역 추출 → (2) 실패하면 첨부 PDF/HWPX 본문 추출
# → (3) 본문에서 키워드 부근 200~500자 압축, 의 3-단계 휴리스틱을 적용한다.
# ────────────────────────────────────────────────────────────────────

SUMMARY_KEYWORDS = (
    '사업개요', '사업목적', '사업내용', '지원내용', '지원대상',
    '사업안내', '모집내용', '신청자격', '공고요지', '추진목적',
    '지원자격', '지원분야', '신청대상', '사업기간', '모집분야',
    '사업소개', '추진배경', '주요내용',
)

_ATTACH_EXT_RE = re.compile(r'\.(pdf|hwpx|hwp|docx?|xlsx?)(?:[?#&]|$)', re.I)
_ATTACH_DOWNLOAD_HINTS = ('download', 'filedown', 'boardfile', 'streamdownload',
                          'fileview', 'fncfiledownload', 'attach', 'getfile')

# 첨부 파일명 우선순위 — 공고문류는 점수 높게, 양식/안내/포스터/별첨 등은 낮게.
# (높은 score를 먼저 처리)
def _attachment_score(filename):
    fn = (filename or '').lower()
    if not fn:
        return 0
    score = 0
    # 본문성 키워드
    for kw, w in (('공고문', 50), ('모집공고', 50), ('공고', 30),
                  ('사업안내', 25), ('안내문', 20), ('faq', 15),
                  ('사업개요', 40), ('사업계획', 25), ('지원사업', 20)):
        if kw in fn:
            score += w
            break
    # 비본문 키워드 (감점)
    for kw, w in (('ksic', -40), ('포스터', -40), ('양식', -30),
                  ('신청서', -25), ('서식', -25), ('동의서', -30),
                  ('체크리스트', -20), ('별첨', -15), ('붙임2', -10),
                  ('붙임3', -15), ('붙임4', -15)):
        if kw in fn:
            score += w
    return score


def _normalize_text(s):
    if not s:
        return ""
    s = s.replace('\xa0', ' ').replace('​', '').replace('﻿', '')
    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    out = '\n'.join(lines)
    out = re.sub(r'[ \t]{2,}', ' ', out)
    return out


def _trim_summary(text, max_len=500):
    text = _normalize_text(text)
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len - 3] + "..."


def extract_summary_from_html(html_or_soup):
    """HTML에서 사업개요/사업목적 키워드 주변 텍스트를 500자 이내로 추출."""
    if isinstance(html_or_soup, str):
        soup = BeautifulSoup(html_or_soup, 'html.parser')
    else:
        soup = html_or_soup

    parts = []

    # 1) <th>/<dt> 라벨 + 인접 <td>/<dd> 패턴
    for label_tag in soup.find_all(['th', 'dt']):
        label = label_tag.get_text(' ', strip=True)
        if not label or not any(kw in label for kw in SUMMARY_KEYWORDS):
            continue
        sib = label_tag.find_next_sibling(['td', 'dd'])
        if sib:
            txt = sib.get_text(' ', strip=True)
            if txt and len(txt) > 8:
                parts.append(f"[{label}] {txt}")

    # 2) 라벨로 시작하는 li/p 텍스트 (○ 사업개요: ..., ㅇ 사업목적 ..., 등)
    if not parts:
        prefix_re = re.compile(r'^[\s○●◎□■▶▷ㅇ\-\d\.\)\]]*(' + '|'.join(SUMMARY_KEYWORDS) + r')\s*[:：\-]')
        for tag in soup.find_all(['li', 'p', 'div']):
            text = tag.get_text(' ', strip=True)
            if not text or len(text) < 12 or len(text) > 1000:
                continue
            if prefix_re.match(text):
                parts.append(text)

    # 3) 게시판 본문 영역 통째로 (가장 큰 컨테이너 우선)
    if not parts:
        for sel in ['div.board-content', 'div.view_cont', 'div.bbs-content',
                    'div.board-view', 'div.detail-content', 'div#bbsView',
                    'div.view-content', 'td.cont', 'td.content',
                    'div.contents-detail', 'div.view_box']:
            el = soup.select_one(sel)
            if el:
                txt = el.get_text(' ', strip=True)
                if txt and len(txt) > 80:
                    parts.append(txt)
                    break

    return _trim_summary(' '.join(parts), max_len=500)


def find_attachment_links(soup, base_url=""):
    """상세 페이지에서 다운로드 가능한 PDF/HWPX/HWP/DOCX/XLSX 첨부 링크를 찾아 반환.

    - <a href> + <a onclick> + 임의 onclick 핸들러 모두 검사
    - URL에 확장자가 없으면 텍스트/형제 노드/onclick 인자에서 확장자 추정
    - 파일명 기반 점수로 공고문류 우선 정렬
    """
    found = []
    seen_urls = set()

    def _detect_ext_from_text(*texts):
        for t in texts:
            if not t:
                continue
            m = _ATTACH_EXT_RE.search(t)
            if m:
                return m.group(1).lower()
        return None

    def _add(filename, url, ext):
        if not url or url in seen_urls:
            return
        seen_urls.add(url)
        found.append((filename, url, ext))

    for a in soup.find_all('a'):
        href = (a.get('href') or '').strip()
        onclick = (a.get('onclick') or '').strip()
        text = a.get_text(' ', strip=True)
        # 형제/상위 텍스트 (파일명이 인접 노드에 표시되는 경우)
        # + a@title/@download — 비즈인포는 텍스트='다운로드'·title='첨부파일 xxx.pdf 다운로드'
        #   구조라 title을 안 읽으면 0건 반환(라이브 실증).
        ancestor_texts = [text, a.get('title') or '', a.get('download') or '']
        cur = a.parent
        for _ in range(3):
            if cur is None:
                break
            ancestor_texts.append(cur.get_text(' ', strip=True))
            cur = cur.parent

        candidates = []  # [(url_or_None, ext, filename)]

        # 1) 일반 href (javascript: 제외)
        if href and not href.startswith('#') and not href.lower().startswith('javascript:'):
            ext = _detect_ext_from_text(href, *ancestor_texts)
            if not ext and any(h in href.lower() for h in _ATTACH_DOWNLOAD_HINTS):
                ext = _detect_ext_from_text(*ancestor_texts)
            if ext:
                full_url = urljoin(base_url, href) if base_url else href
                # 파일명: 조상 텍스트 중 확장자 포함된 것 우선
                fn = text or href.rsplit('/', 1)[-1]
                for at in ancestor_texts:
                    if _ATTACH_EXT_RE.search(at):
                        # at에서 파일명만 추출 (확장자 포함 토큰)
                        m = re.search(r'([\w\[\]\(\)\.\-가-힣 ]+\.(?:pdf|hwpx|hwp|docx?|xlsx?))', at, re.I)
                        if m:
                            fn = m.group(1).strip()
                            break
                candidates.append((full_url, ext, fn))

        # 2) javascript: 콜백 안에 파일명이 있는 경우 — fncFileDownload('bbs', 'foo.pdf')
        js_blob = href if href.lower().startswith('javascript:') else onclick
        if js_blob:
            m = re.search(r"['\"]([^'\"]*\.(pdf|hwpx|hwp|docx?|xlsx?))['\"]", js_blob, re.I)
            if m:
                fn = m.group(1)
                ext = m.group(2).lower()
                # JS 콜백은 다운로드 URL을 동적으로 만들어서, requests로는 호출 불가.
                # 단 표시용으로 파일명만 기록해두면 디버깅에 도움.
                _add(fn, f"js://{fn}", ext)

        for url, ext, fn in candidates:
            _add(fn, url, ext)

    # onclick 속성을 가진 모든 요소(button, span 등) 검사 — 일부 사이트는 a가 아닌 곳에서 다운
    for el in soup.find_all(attrs={'onclick': True}):
        if el.name == 'a':
            continue  # 이미 처리됨
        onclick = el.get('onclick') or ''
        m = re.search(r"['\"]([^'\"]*\.(pdf|hwpx|hwp|docx?|xlsx?))['\"]", onclick, re.I)
        if m:
            fn = m.group(1)
            ext = m.group(2).lower()
            _add(fn, f"js://{fn}", ext)

    # 파일명 점수 + 확장자 우선순위로 정렬
    ext_priority = {'pdf': 0, 'hwpx': 1, 'hwp': 2, 'docx': 3, 'doc': 4, 'xlsx': 5, 'xls': 6}
    # js:// 항목은 후순위 (실제로 fetch 불가)
    found.sort(key=lambda t: (
        t[1].startswith('js://'),
        -_attachment_score(t[0]),
        ext_priority.get(t[2], 9),
    ))
    # js:// 항목은 결과에서 제외 (fetch 불가)
    found = [t for t in found if not t[1].startswith('js://')]
    return found


def _find_attachments_in_raw_html(html, base_url=""):
    """raw HTML에서 download URL + 확장자 패턴을 직접 검색 (a href 외 위치 대응)."""
    found = []
    seen = set()
    # 패턴 A: URL이 .확장자로 끝나는 경우
    re_a = re.compile(
        r'["\'](/?[^"\'\s<>]+?\.(?:pdf|hwpx|hwp|docx?|xlsx?))(?=[?#"\'\s])',
        re.I,
    )
    for m in re_a.finditer(html):
        url = m.group(1)
        if not url.startswith(('http://', 'https://', '/')):
            continue
        em = re.search(r'\.(pdf|hwpx|hwp|docx?|xlsx?)$', url, re.I)
        if not em:
            continue
        ext = em.group(1).lower()
        full_url = urljoin(base_url, url) if base_url and not url.startswith('http') else url
        if full_url in seen:
            continue
        seen.add(full_url)
        fn = url.rsplit('/', 1)[-1]
        found.append((fn, full_url, ext))
    # 패턴 B: download/stream 엔드포인트의 query string에 파일명
    re_b = re.compile(
        r'["\'](/?[^"\'\s<>]*?(?:download|stream|attach|filedown|getfile)[^"\'\s<>]*?[?&]'
        r'(?:fileSaveNm|fileName|filename|filenm|attachNm|fileNm)=([^&"\']+?\.(?:pdf|hwpx|hwp|docx?|xlsx?)))(?=[&"\'])',
        re.I,
    )
    for m in re_b.finditer(html):
        url = m.group(1)
        fn = m.group(2)
        em = re.search(r'\.(pdf|hwpx|hwp|docx?|xlsx?)$', fn, re.I)
        ext = em.group(1).lower() if em else 'pdf'
        full_url = urljoin(base_url, url) if base_url and not url.startswith('http') else url
        if full_url in seen:
            continue
        seen.add(full_url)
        found.append((fn, full_url, ext))

    # 패턴 C: 파일명 텍스트와 인접한 download URL을 페어링
    # (전북TP 같이 <a>에는 '다운로드'만 있고 파일명은 별도 위치에 있는 경우)
    fn_matches = list(re.finditer(
        r'([^\s"\'<>]{2,80}\.(pdf|hwpx|hwp|docx?|xlsx?))(?:\s|\()',
        html, re.I,
    ))
    dl_matches = list(re.finditer(
        r'["\'](/?[^"\'\s<>]*?(?:download|fileDown|stream|attach|getfile)[^"\'\s<>]*?)["\']',
        html, re.I,
    ))
    for dl in dl_matches:
        url = dl.group(1)
        full_url = urljoin(base_url, url) if base_url and not url.startswith('http') else url
        if full_url in seen:
            continue
        # URL 자체에 확장자가 있으면 패턴 A에서 이미 처리됨 → skip
        if _ATTACH_EXT_RE.search(url):
            continue
        # 가장 가까이 (앞쪽으로) 위치한 파일명 찾기
        best_fn = None
        best_dist = 1500
        for fm in fn_matches:
            if fm.end() <= dl.start():
                dist = dl.start() - fm.end()
                if dist < best_dist:
                    best_dist = dist
                    best_fn = fm
        if not best_fn:
            continue
        fn = best_fn.group(1).strip()
        # 파일명에서 한글/영문/숫자/괄호만 남기고 정리
        fn = re.sub(r'[^\w\(\)\[\]가-힣\. \-]', '', fn).strip()
        if len(fn) < 5:
            continue
        ext = best_fn.group(2).lower()
        seen.add(full_url)
        found.append((fn, full_url, ext))

    ext_priority = {'pdf': 0, 'hwpx': 1, 'hwp': 2, 'docx': 3, 'doc': 4, 'xlsx': 5, 'xls': 6}
    found.sort(key=lambda t: (-_attachment_score(t[0]), ext_priority.get(t[2], 9)))
    return found


# 사이트별 다운로드 URL 변환기 — javascript: 콜백을 직접 다운로드 가능한 URL로 매핑
def _itp_attachment_resolver(soup, base_url):
    """인천테크노파크: javascript:fncFileDownload('bbs','xxx.pdf') → 다운로드 URL 추정.

    ITP는 fncFileDownload(boardId, savedFileName)이 form post로 처리하지만,
    /upload/<boardId>/<savedFileName> 패턴으로 직접 접근 가능한 경우가 많다.
    여러 후보 경로를 시도해서 첫 번째 200 응답을 사용.
    """
    found = []
    for a in soup.find_all('a', href=True):
        href = a.get('href', '')
        text = a.get_text(' ', strip=True)
        if not href.lower().startswith('javascript:fncfiledownload'):
            continue
        m = re.search(r"fncFileDownload\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]", href, re.I)
        if not m:
            continue
        board, savedfn = m.group(1), m.group(2)
        em = re.search(r'\.(pdf|hwpx|hwp|docx?|xlsx?)$', savedfn, re.I)
        if not em:
            continue
        ext = em.group(1).lower()
        # 후보 URL 패턴들 (사이트별로 다름)
        candidates = [
            f"https://www.itp.or.kr/upload/{board}/{savedfn}",
            f"https://www.itp.or.kr/upload/bbs/{savedfn}",
            f"https://www.itp.or.kr/uploadbbs/{savedfn}",
            f"https://www.itp.or.kr/data/{board}/{savedfn}",
            f"https://www.itp.or.kr/data/upload/{savedfn}",
        ]
        # 표시용 파일명은 a 태그의 텍스트 우선
        display_fn = text or savedfn
        for cand in candidates:
            found.append((display_fn, cand, ext))
    return found


def extract_text_from_pdf(url, session=None, timeout=15, max_pages=5, referer=None):
    """원격 PDF에서 텍스트를 추출. pdfplumber 미설치 또는 비번 PDF는 빈 문자열."""
    try:
        import pdfplumber
    except ImportError:
        return ""
    s = session or GLOBAL_SESSION
    try:
        # HTML escape entity 정리 (&amp; → &)
        import html as _html
        url = _html.unescape(url)
        headers = {}
        if referer:
            headers['Referer'] = referer
        r = s.get(url, timeout=timeout, verify=False, headers=headers or None)
        if r.status_code != 200 or not r.content:
            return ""
        # PDF 시그니처 검증 (서버가 HTML 에러 페이지 반환했을 수 있음)
        if not r.content.startswith(b'%PDF'):
            return ""
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            pages_text = []
            for i, page in enumerate(pdf.pages):
                if i >= max_pages:
                    break
                try:
                    t = page.extract_text() or ""
                except Exception:
                    t = ""
                pages_text.append(t)
            return _normalize_text('\n'.join(pages_text))
    except Exception:
        return ""


def extract_text_from_hwpx(url, session=None, timeout=15, referer=None):
    """HWPX(zip + xml) 파일에서 텍스트를 추출."""
    s = session or GLOBAL_SESSION
    try:
        import html as _html
        url = _html.unescape(url)
        headers = {'Referer': referer} if referer else None
        r = s.get(url, timeout=timeout, verify=False, headers=headers)
        if r.status_code != 200 or not r.content:
            return ""
        # ZIP 시그니처 검증
        if not r.content.startswith(b'PK'):
            return ""
        texts = []
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            section_names = sorted(
                n for n in zf.namelist()
                if n.startswith('Contents/section') and n.endswith('.xml')
            )
            for name in section_names[:5]:
                try:
                    data = zf.read(name)
                    root = ET.fromstring(data)
                except (ET.ParseError, KeyError, zipfile.BadZipFile):
                    continue
                for elem in root.iter():
                    t = elem.text
                    if t and t.strip():
                        texts.append(t.strip())
        return _normalize_text(' '.join(texts))
    except Exception:
        return ""


def extract_text_from_hwp(url, session=None, timeout=15, referer=None):
    """HWP(구형 OLE) 파일에서 본문 텍스트 추출.

    HWP 5.x은 OLE compound document 안에 BodyText/SectionN 스트림이 있고,
    HWPHeader 의 PROP 비트0이 1이면 raw deflate(zlib raw)로 압축돼 있다.
    Section 안은 HWP record 포맷 — tag/level/size 헤더 4바이트 + 본문.
    PARA_TEXT(tag=HWPTAG_BEGIN+51=67) record 의 UTF-16-LE 디코딩 → 단락 텍스트.
    """
    try:
        import olefile
    except ImportError:
        return ""
    s = session or GLOBAL_SESSION
    try:
        import html as _html
        url = _html.unescape(url)
        headers = {'Referer': referer} if referer else None
        r = s.get(url, timeout=timeout, verify=False, headers=headers)
        if r.status_code != 200 or not r.content:
            return ""
        # OLE 시그니처 검증 (D0CF11E0...)
        if not r.content.startswith(b'\xd0\xcf\x11\xe0'):
            return ""
        ole = olefile.OleFileIO(io.BytesIO(r.content))
        try:
            if not ole.exists('FileHeader'):
                return ""
            header = ole.openstream('FileHeader').read()
            if not header.startswith(b'HWP Document File'):
                return ""
            # Properties: byte 36, bit 0 = compressed, bit 1 = encrypted
            if len(header) <= 36:
                return ""
            props = header[36]
            compressed = (props & 0x01) != 0
            encrypted = (props & 0x02) != 0
            if encrypted:
                return ""

            section_paths = []
            for entry in ole.listdir():
                if (len(entry) == 2 and entry[0] == 'BodyText'
                        and entry[1].lower().startswith('section')):
                    section_paths.append(entry)
            section_paths.sort(key=lambda p: p[1])

            HWPTAG_BEGIN = 0x10
            # HWP record tag — PARA_TEXT = HWPTAG_BEGIN(16) + 51 = 67
            PARA_TEXT_TAG = HWPTAG_BEGIN + 51

            all_texts = []
            for path in section_paths[:6]:  # 처음 6개 섹션만
                try:
                    raw = ole.openstream(path).read()
                    if compressed:
                        try:
                            raw = zlib.decompress(raw, -15)  # raw deflate
                        except zlib.error:
                            continue
                    pos = 0
                    section_texts = []
                    while pos + 4 <= len(raw):
                        h = struct.unpack('<I', raw[pos:pos + 4])[0]
                        tag_id = h & 0x3FF
                        # level = (h >> 10) & 0x3FF  # unused
                        size = (h >> 20) & 0xFFF
                        pos += 4
                        if size == 0xFFF:
                            if pos + 4 > len(raw):
                                break
                            size = struct.unpack('<I', raw[pos:pos + 4])[0]
                            pos += 4
                        if pos + size > len(raw):
                            break
                        rec_data = raw[pos:pos + size]
                        pos += size
                        if tag_id == PARA_TEXT_TAG and size > 0:
                            try:
                                txt = rec_data.decode('utf-16-le', errors='ignore')
                            except Exception:
                                continue
                            # 제어문자/특수문자 제거 (한글 BMP는 보존)
                            cleaned = []
                            for ch in txt:
                                code = ord(ch)
                                if ch in ' \n\t' or 0x20 <= code < 0xD800 or 0xE000 <= code < 0xFFFE:
                                    cleaned.append(ch)
                            t = ''.join(cleaned).strip()
                            if t:
                                section_texts.append(t)
                    if section_texts:
                        all_texts.append(' '.join(section_texts))
                except Exception:
                    continue
            return _normalize_text(' '.join(all_texts))
        finally:
            ole.close()
    except Exception:
        return ""


def summarize_long_text(text, max_len=500):
    """PDF/HWPX 본문에서 사업개요 영역 추출. 키워드 부근 우선, 없으면 앞부분."""
    text = _normalize_text(text)
    if not text:
        return ""
    best_idx, best_kw = -1, None
    for kw in SUMMARY_KEYWORDS:
        idx = text.find(kw)
        if idx >= 0 and (best_idx < 0 or idx < best_idx):
            best_idx, best_kw = idx, kw
    if best_idx >= 0:
        chunk = text[best_idx:best_idx + max_len + 200]
        return _trim_summary(chunk, max_len=max_len)
    # 키워드 못 찾음: 앞부분에서 메타데이터(공고번호/연락처) 건너뛰고 본문 시작 추정
    lines = text.split('\n')
    body_start = 0
    for i, ln in enumerate(lines):
        if len(ln) > 30 and not re.match(r'^[\s\-=─━〓]+$', ln):
            body_start = i
            break
    body = '\n'.join(lines[body_start:body_start + 30])
    return _trim_summary(body, max_len=max_len)


# ════════════════════════════════════════════════════════════════════
# 핵심정보 추출 엔진 — 공고 텍스트(첨부 전문/사업개요)에서 구조화 필드 마이닝
# ════════════════════════════════════════════════════════════════════
# 설계: 패턴 마이닝(B) 골격 + 섹션 헤더 가드(A) 병합. 실측 84~92% 정밀도 기반.
# 모든 마이너는 개별 try/except 격리 — extract_key_fields는 절대 raise하지 않는다.

_KI_HANJA_MAP = {'社': '사', '員': '원', '當': '당'}
_KI_CJK_RUN = re.compile(r'[一-鿿]+')


def _ki_clean_text(text):
    """HWP mojibake(CJK run)·PDF 윤곽선체 중복문자·NBSP 정리."""
    if not text:
        return ''
    for h, k in _KI_HANJA_MAP.items():
        text = text.replace(h, k)
    text = _KI_CJK_RUN.sub(' ', text)
    text = re.sub(r'(.)\1{4,}', r'\1', text)
    text = text.replace('\xa0', ' ')
    text = re.sub(r'[ \t]{2,}', ' ', text)
    # 복합 금액 정규화: '1억4천만원'→'14,000만원' (단일 단위 regex가 절단 오인하는 것 방지)
    text = re.sub(
        r'(\d{1,3})\s*억\s*(\d{1,4})\s*천만\s*원',
        lambda m: format(int(m.group(1)) * 10000 + int(m.group(2)) * 1000, ',') + '만원',
        text)
    return text


_KI_NUM = r'\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?'
_KI_MONEY = re.compile(
    rf'({_KI_NUM})\s*(억\s*원|천만\s*원|백만\s*원|십만\s*원|만\s*원|천\s*원|억(?![가-힣])|원)')
_KI_MULT = {'억원': 1e8, '억': 1e8, '천만원': 1e7, '백만원': 1e6,
            '십만원': 1e5, '만원': 1e4, '천원': 1e3, '원': 1}
_KI_RATE = re.compile(r'(\d{1,3})\s*%\s*(?:이내|까지|한도)?\s*(?:를?\s*)?(지원|보조|국비|감면|환급)')
_KI_PER = re.compile(r'(기업\s*당|개사\s*당|업체\s*당|과제\s*당|팀\s*당|건\s*당|[1l]?\s*인\s*당|1?\s*사\s*당|명\s*당|기관\s*당)')
_KI_QUAL = re.compile(r'(최대|한도|이내|내외|까지|상한|限)')
_KI_AMOUNT_LABEL = re.compile(
    r'(지원\s*금액|지원\s*규모|지원\s*한도|지원\s*단가|총\s*사업비|보조금|지원액|융자|대출\s*한도|보증\s*한도|바우처)')
_KI_AMOUNT_NEG = re.compile(r'(누적\s*투자|투자\s*실적|투자액|투자\s*유치|매출|자본금|수출\s*실적|출자|보증\s*잔액)')

_KI_SCALE = re.compile(
    r'(?:총\s*)?(\d{1,4})\s*(개\s*사|개사|개\s*기업|개\s*업체|개\s*과제|개\s*팀|개\s*기관|개소|명|팀|건)'
    r'(?:\s*(내외|이내|미만))?')
_KI_SCALE_CTX = re.compile(r'(모집|선정|선발|규모|인원|기업\s*수|업체\s*수|채용|지원\s*기업)')

_KI_PHONE = re.compile(r'(?<!\d)(0\d{1,2})[-.)]\s?(\d{3,4})[-.]\s?(\d{4})(?!\d)')
_KI_HOTLINE = re.compile(r'(?:국번\s*없이\s*|☎\s*)(1\d{3})(?:-(\d{4}))?(?!\d)')
_KI_ORG = re.compile(
    r'([가-힣A-Za-z()（）·&\s]{2,28}?(?:팀|센터|진흥원|재단|공사|공단|본부|지원단|사업단|'
    r'협회|연구원|연구소|대학|사무국|테크노파크|상공회의소|진흥회|진흥공단|[가-힣]{1,6}과))')

_KI_ELIG_LABEL = re.compile(
    r'(지원\s*대상|신청\s*자격|지원\s*자격|신청\s*대상|모집\s*대상|모집\s*요건|'
    r'참가\s*자격|참여\s*자격|참가\s*대상|참여\s*대상|모집\s*기업|지원\s*요건)')
_KI_CUT = re.compile(
    r'(지원\s*내용|지원\s*규모|신청\s*기간|사업\s*기간|접수\s*기간|신청\s*방법|'
    r'지원\s*조건|제외\s*대상|[❍◦○□■▶]|\n\s*[-–•*]|\n\s*\d\s*[.)])')
_KI_ELIG_KW = re.compile(
    r'(중소기업|소상공인|중견기업|창업기업|예비\s*창업|스타트업|업력|매출|소재|'
    r'관내|도내|시내|영위|종사|개인사업자|법인|기업|기관)')
_KI_TAG_REGION = re.compile(
    r'([가-힣]{2,8}(?:특별시|광역시|시|도|군|구))\s*(?:소재|관내|내\s*소재|지역)|(도내|관내|시내|지역\s*내)')
_KI_TAG_YEARS = re.compile(r'(?:창업|업력|설립)\s*(?:후\s*)?(\d{1,2})\s*년\s*(이내|미만|이하|이상|초과)')
_KI_TAG_SALES = re.compile(rf'매출(?:액)?\s*(?:이\s*)?({_KI_NUM})\s*(억|백만|천만)?\s*원?\s*(이상|이하|미만|초과)?')

_KI_APPLY_LABEL = re.compile(
    r'(신청\s*방법|접수\s*방법|신청\s*및\s*접수|접수\s*및\s*신청|신청\s*[·∙/]\s*접수|접수처|신청\s*절차)')
_KI_EMAIL = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
_KI_URL = re.compile(r'(?:https?://)?(?:www\.)?[A-Za-z0-9][A-Za-z0-9.-]+\.(?:go|or|co|re)\.kr(?:/[^\s"\'\)<>]*)?')
_KI_SYSTEMS = re.compile(
    r'(SMTECH|스마트공장\s*1번가|K-?[Ss]tartup|케이스타트업|기업마당|소상공인\s*24|'
    r'IRIS|범부처통합연구지원시스템|이지비즈|ezbiz|RIPC|정부24|나라장터|e나라도움|보조금24)', re.I)
_KI_CH_PATTERNS = [
    ('온라인', re.compile(r'온라인\s*(?:신청|접수|제출)?|홈페이지(?:를\s*통해|에서)|누리집|시스템.{0,8}(?:신청|접수|등록|입력)')),
    ('이메일', re.compile(r'이메일|전자\s*우편|e-?mail', re.I)),
    ('방문', re.compile(r'방문\s*(?:접수|제출|신청)')),
    ('우편', re.compile(r'우편\s*(?:접수|제출|송부)')),
    ('팩스', re.compile(r'팩스\s*(?:접수|제출)')),
]
_KI_CONTENT_LABEL = re.compile(r'(지원\s*내용|지원\s*사항|지원\s*프로그램|지원\s*항목|사업\s*내용)')
_KI_CONTACT_LABEL = re.compile(r'(문의처|연락처|문\s*의)')
_KI_HOMETAX_BLOCK = re.compile(r'hometax\.go\.kr|nts\.go\.kr', re.I)

_KI_JOSA = ('을', '를', '이', '가', '은', '는', '의', '에', '으로', '로', '과', '와', '도', '만')
# 필드 캡(자): 합산 최악 ~820자 ≈ 2.5KB/건
_KI_FIELD_CAPS = {'지원금액': 60, '지원금액_총': 40, '선정규모': 40, '지원대상': 150,
                  '지원내용': 250, '신청방법': 100, '문의처': 100}


def _ki_cap(s, n):
    s = (s or '').strip()
    return s if len(s) <= n else s[:n - 1].rstrip() + '…'


def _ki_fmt_krw(v):
    if v >= 1e8:
        s = f'{v / 1e8:.2f}'.rstrip('0').rstrip('.')
        return s + '억원'
    if v >= 1e4:
        return format(int(round(v / 1e4)), ',') + '만원'
    return format(int(v), ',') + '원'


def _ki_is_header_shaped(text, start, end, strict=True):
    """라벨 매치가 '섹션 헤더 모양'인지 판정 (심판 필수수정 — mid-sentence 함정 차단).
    (a) 직전 문자가 한글 음절이면 단어 중간 매칭, (b) 직후 토큰이 조사면 본문 문장,
    (c) strict: 같은 물리행 잔여가 콜론/대시 시작 또는 ≤12자일 때만 헤더 인정.
    """
    if start > 0 and '가' <= text[start - 1] <= '힣':
        return False
    rest = text[end:end + 16].lstrip(' \t')
    if rest and any(rest.startswith(j) for j in _KI_JOSA):
        return False
    # (b') 직후가 '명사+조사' 꼴이면 본문 문장 — '지원 대상 여부를 판단…' 함정 차단.
    # (c)를 개요에 적용하면 한 줄 흐름 텍스트의 정상 라벨까지 차단(실측 대상 27%→36% 회복).
    if rest and re.match(r'^[가-힣]{1,4}(?:을|를|이|가|은|는)(?:\s|$)', rest):
        return False
    if strict:
        nl = text.find('\n', end)
        line_rest = text[end:nl if nl != -1 else end + 40].strip()
        if line_rest and not (line_rest[0] in ':：)-–—]' or len(line_rest) <= 12):
            return False
    return True


_KI_SECTION_LABELS = (
    ('지원대상', _KI_ELIG_LABEL),
    ('지원내용', _KI_CONTENT_LABEL),
    ('신청방법', _KI_APPLY_LABEL),
    ('문의처', _KI_CONTACT_LABEL),
    ('지원규모', re.compile(r'(지원\s*규모|사업\s*규모|선정\s*규모)')),
)


def _ki_split_sections(text, strict=True):
    """{섹션명: (start, end)} — 헤더모양 가드 통과 매치만 헤더로 인정, 본문 최대 600자."""
    heads = []
    for name, rx in _KI_SECTION_LABELS:
        for m in rx.finditer(text):
            if _ki_is_header_shaped(text, m.start(), m.end(), strict=strict):
                heads.append((m.start(), m.end(), name))
                break
    heads.sort()
    out = {}
    for i, (s, e, name) in enumerate(heads):
        nxt = heads[i + 1][0] if i + 1 < len(heads) else len(text)
        out[name] = (e, min(nxt, e + 600))
    return out


def _ki_label_window(text, label_re, width=170, strict=True):
    """라벨 직후 본문 윈도우 추출 (헤더모양 가드 적용)."""
    for m in label_re.finditer(text):
        if not _ki_is_header_shaped(text, m.start(), m.end(), strict=strict):
            continue
        chunk = text[m.end():m.end() + width + 40]
        chunk = re.sub(r'^[\s):：\-\]]+', '', chunk)
        cut = _KI_CUT.search(chunk, 8)
        if cut:
            chunk = chunk[:cut.start()]
        chunk = chunk.replace('\n', ' ').strip()
        if len(chunk) > 12:
            return chunk[:width]
    return None


def _ki_mine_amount(text, bonus_ranges=()):
    """지원금액 마이닝. 심판 수정: ①tie-break per-인접도 ②총/per 채널 분리 +섹션보너스 +융자라벨."""
    out = []
    for m in _KI_MONEY.finditer(text):
        num_s, unit = m.group(1), re.sub(r'\s', '', m.group(2))
        try:
            krw = float(num_s.replace(',', '')) * _KI_MULT[unit]
        except (ValueError, KeyError):
            continue
        if unit == '원' and krw < 100000:
            continue
        if krw < 10000 or krw > 1e13:
            continue
        s, e = m.span()
        before, after = text[max(0, s - 28):s], text[e:e + 14]
        line_s = text.rfind('\n', 0, s) + 1
        line = text[line_s:text.find('\n', e) if text.find('\n', e) > 0 else e + 40]
        per_m, per_dist = _KI_PER.search(before), 9999
        # per 마커와 금액 사이에 구분자·'총'이 끼면 다른 금액의 마커가 샌 것 —
        # '( 1건당 1000만원 이내, 총 1억…)'에서 총액이 건당으로 오추출되는 버그 차단.
        if per_m and re.search(r'[,，;/·총]|및', before[per_m.end():]):
            per_m = None
        if per_m:
            per_dist = len(before) - per_m.end()
        else:
            am = _KI_PER.search(after[:8])
            if am and not re.search(r'[,，;/·]|및', after[:am.start()]):
                per_m, per_dist = am, am.start()
        qual_m = _KI_QUAL.search(before[-14:] + ' ' + after)
        label_m = _KI_AMOUNT_LABEL.search(line)
        label_txt = re.sub(r'\s', '', label_m.group(0)) if label_m else ''
        loan = bool(re.match(r'(융자|대출|보증)', label_txt))
        total = bool(re.search(r'총\s*$|총\s*사업비', before))
        score = (3 if per_m else 0) + (2 if qual_m else 0) + (2 if label_m else 0) \
            + (1 if krw >= 1e6 else 0) + (1 if total else 0)
        if _KI_AMOUNT_NEG.search(before):
            score -= 5
        if any(a <= s < b for a, b in bonus_ranges):
            score += 2
        if loan:
            disp = '융자한도 ' + _ki_fmt_krw(krw) \
                + (' ' + qual_m.group(0) if qual_m and qual_m.group(0) in ('이내', '내외', '까지') else '')
        else:
            disp = ''.join(filter(None, [
                (per_m.group(0).replace(' ', '') + ' ') if per_m else ('총 ' if total else ''),
                (qual_m.group(0) + ' ') if qual_m and qual_m.group(0) in ('최대', '한도') else '',
                _ki_fmt_krw(krw),
                ' ' + qual_m.group(0) if qual_m and qual_m.group(0) in ('이내', '내외', '까지', '상한') else '',
            ]))
        out.append({'krw': krw, 'disp': disp.strip(), 'score': score, 'per_dist': per_dist,
                    'per': bool(per_m), 'total': total})
    # tie-break: score 동률 시 per-마커 인접도(거리 최소) 우선 — '-krw 최대' 오추출 버그 수정
    out.sort(key=lambda d: (-d['score'], d['per_dist'], -d['krw']))
    per_d = next((d for d in out if d['per'] and d['score'] >= 2), None)
    total = next((d['disp'] for d in out if d['total'] and not d['per']), None)
    # 총액 단독(per 없음)은 지원금액(per 채널)으로 절대 승격 금지
    best_d = next((d for d in out if d['score'] >= 2 and not (d['total'] and not d['per'])), None)
    rate_m = _KI_RATE.search(text)
    return {'per': per_d['disp'] if per_d else None,
            'per_krw': per_d['krw'] if per_d else 0,
            'total': total,
            'best': best_d['disp'] if best_d else None,
            'best_krw': best_d['krw'] if best_d else 0,
            'rate': f"{rate_m.group(1)}% {rate_m.group(2)}" if rate_m else None}


def _ki_mine_scale(text):
    """선정규모 마이닝. 단위 union('건' 추가), 명/팀/건은 모집·선정 문맥 필수."""
    cands = []
    for m in _KI_SCALE.finditer(text):
        s, e = m.span()
        if re.search(r'제\s*$', text[max(0, s - 3):s]):
            continue
        if re.search(r'^\s*(차|회|년|월|일|개월|당)', text[e:e + 3]):
            continue
        window = text[max(0, s - 32):e + 18]
        unit = re.sub(r'\s', '', m.group(2))
        if unit in ('명', '팀', '건') and not re.search(r'(모집|선정|선발|채용)', window):
            continue
        score = 0
        if m.group(3):
            score += 2
        if _KI_SCALE_CTX.search(window):
            score += 2
        if re.search(r'(규모|인원)\s*[):：\]]?\s*$', text[max(0, s - 14):s]):
            score += 2
        cands.append({'disp': f"{m.group(1)}{unit}" + (f" {m.group(3)}" if m.group(3) else ''),
                      'score': score})
    cands.sort(key=lambda d: -d['score'])
    return cands[0]['disp'] if cands and cands[0]['score'] >= 2 else None


def _ki_mine_contact(text, extra_scans=()):
    """문의처: ①전화+같은줄 기관 페어링(팩스 제외) ②'문의' 윈도우 ③신청방법 섹션 ④이메일.
    폴백 결과 12자 미만은 쓰레기값으로 폐기 (심판 병합 권고 직렬 체인)."""
    found, seen = [], set()
    for m in _KI_PHONE.finditer(text):
        digits = ''.join(m.groups())
        if digits in seen:
            continue
        pre = text[max(0, m.start() - 12):m.start()]
        if re.search(r'(팩스|FAX|Fax)\s*[:：)]?\s*$', pre):
            continue
        seen.add(digits)
        line_s = max(text.rfind('\n', 0, m.start()), 0)
        orgs = list(_KI_ORG.finditer(text[line_s:m.start()]))
        org = re.sub(r'^[\s()（）·&]+', '', orgs[-1].group(1).strip()) if orgs else ''
        found.append((org + ' ' + f"{m.group(1)}-{m.group(2)}-{m.group(3)}").strip())
    for m in _KI_HOTLINE.finditer(text):
        num = m.group(1) + (('-' + m.group(2)) if m.group(2) else '')
        if num not in seen:
            seen.add(num)
            found.append(f"통합콜센터 {num} (국번없이)")
    if found:
        return found[0]
    # A 폴백 체인
    scans = [_ki_label_window(text, _KI_CONTACT_LABEL, width=110, strict=False)]
    scans.extend(extra_scans)
    for scan in scans:
        if not scan:
            continue
        pm = _KI_PHONE.search(scan)
        if pm:
            orgs = list(_KI_ORG.finditer(scan[:pm.start()]))
            org = orgs[-1].group(1).strip() if orgs else ''
            cand = (org + ' ' + f"{pm.group(1)}-{pm.group(2)}-{pm.group(3)}").strip()
            if len(cand) >= 12:
                return cand
        em = _KI_EMAIL.search(scan)
        if em and len(em.group(0)) >= 12:
            return em.group(0)
    em = _KI_EMAIL.search(text)
    if em and len(em.group(0)) >= 12:
        ctx_w = text[max(0, em.start() - 60):em.end() + 20]
        if re.search(r'(문의|연락)', ctx_w):
            return em.group(0)
    return None


def _ki_mine_target(text, strict=True):
    snippet = _ki_label_window(text, _KI_ELIG_LABEL, strict=strict)
    if not snippet:
        scored = []
        for sg in re.split(r'\n|(?=[❍◦○□■▶ㅇ])', text):
            sg = sg.strip()
            if not (15 <= len(sg) <= 220):
                continue
            kws = set(_KI_ELIG_KW.findall(sg))
            if len(kws) >= 2:
                scored.append((len(kws), sg))
        scored.sort(key=lambda t: -t[0])
        snippet = scored[0][1][:170] if scored else None
    tags = {}
    scan = snippet or text[:1500]
    rm = _KI_TAG_REGION.search(scan)
    if rm:
        tags['region'] = (rm.group(1) or rm.group(2))
    ym = _KI_TAG_YEARS.search(scan) or _KI_TAG_YEARS.search(text)
    if ym:
        tags['years'] = f"창업 {ym.group(1)}년 {ym.group(2)}"
    sm = _KI_TAG_SALES.search(scan) or _KI_TAG_SALES.search(text)
    if sm:
        tags['sales'] = f"매출 {sm.group(1)}{sm.group(2) or ''}원 {sm.group(3) or ''}".strip()
    for kw in ('소상공인', '중소·중견기업', '중소기업', '중견기업', '예비창업자', '개인사업자'):
        if kw in scan:
            tags['type'] = kw
            break
    return {'snippet': snippet, 'tags': tags}


def _ki_mine_apply(text, strict=True):
    snippet = _ki_label_window(text, _KI_APPLY_LABEL, width=150, strict=strict)
    scan = snippet or text
    channels = [name for name, rx in _KI_CH_PATTERNS if rx.search(scan)]
    if not channels and snippet is None:
        channels = [name for name, rx in _KI_CH_PATTERNS if rx.search(text)]
    sys_m = _KI_SYSTEMS.search(text)
    url_m = next((u for u in _KI_URL.finditer(scan) if not _KI_HOMETAX_BLOCK.search(u.group(0))), None)
    if not url_m:
        for um in _KI_URL.finditer(text):
            if _KI_HOMETAX_BLOCK.search(um.group(0)):
                continue
            ctx_w = text[max(0, um.start() - 60):um.end() + 60]
            if re.search(r'(신청|접수|홈페이지|시스템|포털|누리집)', ctx_w):
                url_m = um
                break
    email_m = _KI_EMAIL.search(scan) or _KI_EMAIL.search(text)
    return {'snippet': snippet, 'channels': channels,
            'system': sys_m.group(1) if sys_m else None,
            'url': url_m.group(0) if url_m else None,
            'email': email_m.group(0) if email_m else None}


def extract_key_fields(text, source_kind='attach'):
    """공고 텍스트(첨부 전문 또는 사업개요)에서 핵심정보 dict 추출.

    반환: {지원금액, 지원금액_총, 선정규모, 지원대상, 지원대상_태그, 지원내용,
    신청방법, 문의처}의 부분집합 — 빈 값 키는 생략, 전 필드 optional.
    절대 raise하지 않음(마이너별 격리). source_kind: 'attach'|'overview'.
    """
    try:
        t = _ki_clean_text(text or '')
        if not t or len(t) < 20:
            return {}
        # strict((c)행 규칙)는 개행 구조가 있는 첨부 전문(PDF)에만 — 개요(한 줄 흐름)나
        # HWPX/HWP 평탄화 텍스트에 적용하면 정상 라벨까지 차단(실측 V1/V2 비교로 확정).
        strict = (source_kind != 'overview') and (t.count('\n') / max(len(t), 1) >= 1.0 / 200)
        try:
            sections = _ki_split_sections(t, strict=strict)
        except Exception:
            sections = {}
        bonus = [sections[k] for k in ('지원규모', '지원내용') if k in sections]
        apply_section = ''
        if '신청방법' in sections:
            a, b = sections['신청방법']
            apply_section = t[a:b]
        fields = {}
        try:
            amount = _ki_mine_amount(t, bonus_ranges=bonus)
        except Exception:
            amount = {'per': None, 'per_krw': 0, 'total': None, 'best': None, 'best_krw': 0, 'rate': None}
        chosen = amount['per'] or amount['best']
        chosen_krw = amount['per_krw'] or amount['best_krw']
        if amount['rate'] and (not chosen or chosen_krw < 1e6):
            fields['지원금액'] = _ki_cap(amount['rate'] + (f" (+{chosen})" if chosen else ''), 60)
        elif chosen:
            fields['지원금액'] = _ki_cap(chosen, 60)
        if amount['total']:
            fields['지원금액_총'] = _ki_cap(amount['total'], 40)
        try:
            scale = _ki_mine_scale(t)
        except Exception:
            scale = None
        if scale:
            fields['선정규모'] = _ki_cap(scale, 40)
        try:
            target = _ki_mine_target(t, strict=strict)
        except Exception:
            target = {'snippet': None, 'tags': {}}
        if target['snippet']:
            fields['지원대상'] = _ki_cap(target['snippet'], 150)
            if target['tags']:
                fields['지원대상_태그'] = {k: str(v)[:20] for k, v in target['tags'].items()}
        try:
            ap = _ki_mine_apply(t, strict=strict)
        except Exception:
            ap = {'snippet': None, 'channels': [], 'system': None, 'url': None, 'email': None}
        if ap['channels'] or ap['snippet']:
            parts = []
            if ap['channels']:
                parts.append('/'.join(ap['channels']))
            ref = ap['system'] or ap['url'] or ap['email']
            if ref:
                parts.append(f"({ref})")
            fields['신청방법'] = _ki_cap(' '.join(parts) if parts else (ap['snippet'] or '')[:80], 100)
        try:
            contact = _ki_mine_contact(t, extra_scans=(apply_section,))
        except Exception:
            contact = None
        if contact:
            fields['문의처'] = _ki_cap(contact, 100)
        try:
            content = _ki_label_window(t, _KI_CONTENT_LABEL, width=240, strict=strict)
        except Exception:
            content = None
        if content:
            fields['지원내용'] = _ki_cap(content, 250)
        return fields
    except Exception:
        return {}


def _fetch_best_attachment_text(soup, html, link, base_url, ctx=None, max_files=2, max_pdf_pages=5):
    """상세 페이지에서 최우선 첨부(공고문류) 본문 텍스트 추출. 반환 (파일명, 전문).
    탐색 순서: find_attachment_links → ctx['attachment_resolver'] → raw-HTML 폴백.
    """
    ctx = ctx or {}
    session = ctx.get('session')
    attachments = find_attachment_links(soup, base_url=base_url)
    site_resolver = ctx.get('attachment_resolver')
    if site_resolver:
        try:
            extra = site_resolver(soup, base_url) or []
            attachments = list(attachments) + list(extra)
        except Exception:
            pass
    if not attachments:
        attachments = list(_find_attachments_in_raw_html(html, base_url))
    for fname, furl, ext in attachments[:max_files]:
        txt = ""
        # Referer를 상세 페이지로 설정 — 일부 사이트는 referer 검증으로 차단
        if ext == 'pdf':
            txt = extract_text_from_pdf(furl, session=session, referer=link, max_pages=max_pdf_pages)
        elif ext == 'hwpx':
            txt = extract_text_from_hwpx(furl, session=session, referer=link)
        elif ext == 'hwp':
            txt = extract_text_from_hwp(furl, session=session, referer=link)
        if txt and len(txt) > 100:
            return fname, txt
    return '', ''


def _load_existing_extras():
    """이전 data.json에서 링크→{핵심정보, _ext} 캐시 (스키마 버전 일치분만).
    copy-forward가 모든 fetch보다 선행 → 차단 run에도 이전 ok 데이터 비파괴.
    """
    web_data_path = os.path.join(os.path.dirname(__file__), 'biz_support_web', 'data.json')
    out = {}
    try:
        with open(web_data_path, 'r', encoding='utf-8') as f:
            for it in json.load(f).get('data', []):
                link = (it.get('링크') or '').strip()
                ki, ext = it.get('핵심정보'), it.get('_ext')
                if not link or not (ki or ext):
                    continue
                if ext and ext.get('v') not in (None, KEYINFO_VERSION):
                    continue  # 버전 bump → 통제된 재백필
                out[link] = {'핵심정보': ki, '_ext': ext}
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return out


def extract_key_info_pass(items, cap=None, deadline_sec=None, bizinfo_ok=True, cache=None):
    """[별도 pass — save_to_web_json 직전 호출] 카드에 '핵심정보'(+'_ext')를 채운다.
    기존 6필드(지원사업명/신청기간/링크/사업개요/출처/first_seen)는 절대 수정하지 않음.

    1단계: 전 항목 사업개요 오프라인 마이닝(네트워크 0, ~1초).
    2단계: 문의처·신청방법·지원내용 결측 항목만 첨부 다운로드.
           PR1은 비즈인포 도메인 한정, EXTRACT_WORKERS 병렬 + 건당 0.5s 간격,
           cap/deadline + 도메인 장애 격리(연속 8실패 또는 실패율>50% 시 잔여 중단).
    _ext: {v:스키마버전, st:ok|none|fail, n:시도횟수, at:날짜} — ok는 fail로 덮이지 않고,
    같은 날 재시도 금지 + n≥4 영구 제외(day-backoff).
    """
    cap = EXTRACT_CAP if cap is None else cap
    deadline_sec = EXTRACT_DEADLINE_SEC if deadline_sec is None else deadline_sec
    today_str = datetime.now().strftime('%Y-%m-%d')
    if cache is None:
        cache = _load_existing_extras()
    # ① 캐시 copy-forward
    for it in items:
        c = cache.get((it.get('링크') or '').strip())
        if c:
            if c.get('핵심정보') and not it.get('핵심정보'):
                it['핵심정보'] = c['핵심정보']
            if c.get('_ext') and not it.get('_ext'):
                it['_ext'] = c['_ext']
    # ② 1단계: 사업개요 오프라인 마이닝
    mined = 0
    for it in items:
        if it.get('핵심정보'):
            continue
        ov = it.get('사업개요') or ''
        if len(ov) < 40:
            continue
        f = extract_key_fields(ov, source_kind='overview')
        if f:
            it['핵심정보'] = f
            mined += 1
    # ③ 2단계: 첨부 보강
    def _needs_attach(it):
        ki = it.get('핵심정보') or {}
        return not (ki.get('문의처') and ki.get('신청방법') and ki.get('지원내용'))

    def _ext_allows(it):
        ext = it.get('_ext') or {}
        if ext.get('st') == 'ok' and ext.get('v') == KEYINFO_VERSION:
            return False
        if ext.get('at') == today_str:
            return False
        if int(ext.get('n') or 0) >= 4:
            return False
        return True

    cands = [it for it in items
             if bizinfo_ok and 'bizinfo.go.kr' in (it.get('링크') or '')
             and _needs_attach(it) and _ext_allows(it)]
    cands.sort(key=lambda it: 0 if not it.get('_ext') else 1)  # 신규 우선
    cands = cands[:max(0, cap)]
    t0 = time.monotonic()
    stats = {'ok': 0, 'none': 0, 'fail': 0, 'skip': 0}
    state = {'consec_fail': 0, 'aborted': False}

    def _one(it):
        if state['aborted'] or time.monotonic() - t0 > deadline_sec:
            stats['skip'] += 1
            return
        link = it.get('링크') or ''
        try:
            html = fetch_html(link, timeout=15)
            soup = BeautifulSoup(html, 'html.parser')
            _fn, txt = _fetch_best_attachment_text(
                soup, html, link, base_url='https://www.bizinfo.go.kr',
                ctx={}, max_files=2, max_pdf_pages=10)
            n = int((it.get('_ext') or {}).get('n') or 0) + 1
            if txt:
                f = extract_key_fields(txt, source_kind='attach')
                if f:
                    merged = dict(it.get('핵심정보') or {})
                    merged.update(f)  # 첨부 전문이 개요 마이닝보다 우선
                    it['핵심정보'] = merged
                    it['_ext'] = {'v': KEYINFO_VERSION, 'st': 'ok', 'n': n, 'at': today_str}
                    stats['ok'] += 1
                else:
                    it['_ext'] = {'v': KEYINFO_VERSION, 'st': 'none', 'n': n, 'at': today_str}
                    stats['none'] += 1
            else:
                it['_ext'] = {'v': KEYINFO_VERSION, 'st': 'none', 'n': n, 'at': today_str}
                stats['none'] += 1
            state['consec_fail'] = 0
        except Exception:
            stats['fail'] += 1
            state['consec_fail'] += 1
            n_prev = int((it.get('_ext') or {}).get('n') or 0)
            it['_ext'] = {'v': KEYINFO_VERSION, 'st': 'fail', 'n': n_prev + 1, 'at': today_str}
            done = stats['ok'] + stats['none'] + stats['fail']
            if state['consec_fail'] >= 8 or (done >= 10 and stats['fail'] / done > 0.5):
                state['aborted'] = True  # 도메인 장애 격리 — 잔여 항목 _ext 미갱신
        finally:
            time.sleep(0.5)

    if cands:
        print(f"[핵심정보] 첨부 보강 대상 {len(cands)}건 (cap={cap}, workers={EXTRACT_WORKERS})")
        with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS) as pool:
            list(pool.map(_one, cands))
    total_ki = sum(1 for it in items if it.get('핵심정보'))
    print(f"[핵심정보] 개요 마이닝 {mined}건 / 첨부 ok {stats['ok']}·none {stats['none']}"
          f"·fail {stats['fail']}·skip {stats['skip']}"
          f"{' (도메인 장애로 중단)' if state['aborted'] else ''}"
          f" / 총 보유 {total_ki}/{len(items)}건")


def backfill_key_info(cap=2000):
    """[로컬 운영용 백필] 저장소 data.json(../data.json)에 핵심정보를 직접 채운다.
    Actions 러너 IP는 비즈인포에 간헐 차단되므로 백필은 로컬(한국 IP)에서 실행:
      EXTRACT_CAP=2000 venv/bin/python -c "import sys; sys.path.insert(0,'crawler');
      import unified_crawler as uc; uc.backfill_key_info()"
    last_updated·기존 필드·항목 순서는 건드리지 않는다(순수 additive).
    """
    path = os.path.join(os.path.dirname(__file__), '..', 'data.json')
    with open(path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    items = payload.get('data', [])
    cache = {}
    for it in items:
        link = (it.get('링크') or '').strip()
        if link and (it.get('핵심정보') or it.get('_ext')):
            cache[link] = {'핵심정보': it.get('핵심정보'), '_ext': it.get('_ext')}
    extract_key_info_pass(items, cap=cap, cache=cache)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[핵심정보] 백필 저장 완료: {path} ({len(items)}건)")


def enrich_detail(item, ctx=None):
    """item['링크'] 페이지를 fetch → HTML/첨부 → 사업개요 채움. item을 in-place 수정."""
    ctx = ctx or {}
    link = (item.get('링크') or '').strip()
    if not link:
        return item

    existing = (item.get('사업개요') or '').strip()
    # 이미 충분히 긴 사업개요가 있으면 skip (다른 사이트의 기존 결과 보호).
    # '내용 추출 실패'/'크롤링 오류'/'링크 없음' 등 placeholder는 무시하고 재시도.
    if len(existing) >= 80 and not any(
        p in existing for p in ('추출 실패', '크롤링 오류', '링크 없음')
    ):
        return item
    if any(p in existing for p in ('추출 실패', '크롤링 오류', '링크 없음')):
        item['사업개요'] = ''  # placeholder 제거

    session = ctx.get('session') or GLOBAL_SESSION
    base_url = ctx.get('base_url') or ('/'.join(link.split('/')[:3]) if link.startswith('http') else '')

    html = ""
    driver = ctx.get('driver')
    if driver is not None:
        try:
            driver.get(link)
            time.sleep(ctx.get('spa_wait', 1.5))
            html = driver.page_source
        except Exception:
            return item
    else:
        # 일시적 5xx/네트워크 에러는 1회 재시도. fetch_html이 Scrapling Fetcher
        # (TLS 스푸핑) 우선, 실패 시 session으로 fallback. CBTP처럼 DESAdapter
        # session이 명시된 경우는 Scrapling 건너뛰고 session 직접 사용.
        # GLOBAL_SESSION은 명시 안 한 것과 동일하게 처리해서 Scrapling 경로를 활성화.
        fetch_session = None if session is GLOBAL_SESSION else session
        for attempt in range(2):
            try:
                html = fetch_html(link, session=fetch_session, timeout=10)
                if html:
                    break
                return item
            except requests.exceptions.HTTPError as he:
                code = getattr(getattr(he, 'response', None), 'status_code', 0)
                if code in (500, 502, 503, 504) and attempt == 0:
                    time.sleep(1.5)
                    continue
                return item
            except Exception:
                if attempt == 0:
                    time.sleep(1.0)
                    continue
                return item

    if not html:
        return item
    soup = BeautifulSoup(html, 'html.parser')

    summary = ""
    custom = ctx.get('html_processor')
    if custom:
        try:
            summary = custom(soup) or ""
        except Exception:
            summary = ""
    if not summary or len(summary) < 50:
        summary = extract_summary_from_html(soup) or summary

    # HTML에서 충분히 못 얻으면 첨부파일 시도 (PDF > HWPX > HWP 순)
    if (not summary or len(summary) < 60) and ctx.get('use_attachments', True):
        _fn, txt = _fetch_best_attachment_text(soup, html, link, base_url, ctx=ctx)
        if txt:
            summary = summarize_long_text(txt, max_len=500)

    if summary:
        item['사업개요'] = summary
    return item


def crawl_generic_details(items, max_workers=4, ctx=None):
    """범용 상세 enrichment — Phase 2 사이트들 공용."""
    if not items:
        return items

    def _one(it):
        try:
            return enrich_detail(it, ctx=ctx)
        except Exception:
            return it

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        return list(ex.map(_one, items))


def _bizinfo_extract_summary(soup):
    """비즈인포 신 상세페이지(selectSIIA200Detail)에서 '사업개요' 추출.

    구조: div.view_cont > ul > li 안에 span.s_title('사업개요') + div.txt.
    requests로 받은 정적 HTML에서 그대로 추출된다 (Selenium 불필요).
    """
    vc = soup.find(class_='view_cont')
    if not vc:
        return ""
    for li in vc.find_all('li'):
        st = li.find(class_='s_title')
        if st and '사업개요' in st.get_text():
            txt = li.find(class_='txt')
            if txt:
                return txt.get_text(' ', strip=True)
    return ""


def _load_existing_summaries():
    """기존 data.json에서 링크→사업개요 캐시 로드 (점진적 보강용).

    이미 충분히 긴 사업개요가 있는 링크는 다음 실행에서 재크롤링하지 않고
    그대로 재사용 → 매일 신규 공고만 상세 fetch 하므로 런타임이 안정적이다.
    placeholder/짧은 값은 캐시하지 않아 다음에 다시 시도된다.
    """
    path = os.path.join(os.path.dirname(__file__), 'biz_support_web', 'data.json')
    cache = {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for it in json.load(f).get('data', []):
                link = (it.get('링크') or '').strip()
                summ = (it.get('사업개요') or '').strip()
                if (link and len(summ) >= 80 and
                        not any(p in summ for p in ('추출 실패', '크롤링 오류',
                                                    '링크 없음', '내용 없음'))):
                    cache[link] = summ
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    except Exception:
        pass
    return cache


def _format_period(start, end):
    """엑셀의 신청시작/종료일자 → 카드 신청기간 문자열."""
    s = (str(start).strip() if start else '')
    e = (str(end).strip() if end else '')
    if s and e:
        return f"{s} ~ {e}"
    if e:
        return f"~ {e}"
    if s:
        return f"{s} ~"
    return "상시"


def _load_prev_bizinfo_items():
    """비즈인포 엑셀 수집 실패 시, 직전 data.json의 비즈인포 항목 중
    아직 마감되지 않은 것을 보존용으로 반환한다.

    비즈인포는 전체 수집량의 ~70%를 차지하는 단일 대형 출처다. GitHub Actions
    러너 IP가 차단돼 엑셀(selectSIIA200ExcelDownload.do)을 못 받으면 그날 전체가
    sanity 가드(전체 70% 미만 거부)에 걸려 TP 사이트 신규 공고까지 통째로
    누락된다. 직전 비즈인포 데이터(미마감)를 유지하면 항목 수가 보존돼 가드를
    통과하고, TP 사이트는 정상 갱신되며, 비즈인포는 차단 해제 시 자동 최신화된다.

    반환 항목은 이미 '출처'='비즈인포'로 태깅돼 있어 main()이 보존 분기를
    식별하고 상세 보강(차단된 서버 재접속)을 건너뛴다.
    """
    web_data_path = os.path.join(os.path.dirname(__file__), 'biz_support_web', 'data.json')
    try:
        with open(web_data_path, 'r', encoding='utf-8') as f:
            prev = json.load(f).get('data', [])
    except (FileNotFoundError, json.JSONDecodeError):
        print("[비즈인포] 보존할 이전 data.json 없음 → 빈 결과")
        return []

    today = datetime.now().date()
    preserved, dropped = [], 0
    for it in prev:
        if it.get('출처') != '비즈인포':
            continue
        # 신청기간 "YYYY-MM-DD ~ YYYY-MM-DD"의 종료일로 마감 판별. 파싱 실패 시 보존(보수적).
        end_str = str(it.get('신청기간', '')).split('~')[-1].strip()[:10]
        try:
            if datetime.strptime(end_str, '%Y-%m-%d').date() < today:
                dropped += 1
                continue
        except Exception:
            pass
        p = {
            '지원사업명': it.get('지원사업명', ''),
            '신청기간': it.get('신청기간', ''),
            '링크': it.get('링크', ''),
            '사업개요': it.get('사업개요', ''),
            '등록일자': it.get('등록일자', ''),
            '출처': '비즈인포',
        }
        # 핵심정보/_ext도 보존 — 미보존 시 비즈인포 차단일마다 신필드가 통째로
        # 증발해 익일 전체 재추출 폭주가 생긴다.
        if it.get('핵심정보'):
            p['핵심정보'] = it['핵심정보']
        if it.get('_ext'):
            p['_ext'] = it['_ext']
        preserved.append(p)
    print(f"[비즈인포] 이전 데이터 보존: {len(preserved)}건 (마감 제외 {dropped}건)")
    return preserved


def _bizinfo_excel_bytes():
    """비즈인포 엑셀 bytes 수신 — (1차) 직접, (2차) 직접 실패 시 릴레이 재시도.

    릴레이 재시도 성공 시 _BIZINFO_STATE['relay']=True가 유지되어 이후 상세페이지
    enrich·첨부 다운로드도 _maybe_relay로 자동 우회한다. 릴레이 미설정이면 직접
    실패 예외를 그대로 올린다(현행 동일).
    """
    try:
        return fetch_bytes(BIZINFO_EXCEL_URL, params={'rows': 15, 'cpage': 1}, timeout=40)
    except Exception as e:
        if not BIZINFO_RELAY_BASE:
            raise
        print(f"[비즈인포] 직접 수신 실패({e}) → 릴레이 재시도")
        _BIZINFO_STATE['relay'] = True  # 이후 www.bizinfo.go.kr fetch 전부 릴레이 경유
        content = fetch_bytes(BIZINFO_EXCEL_URL, params={'rows': 15, 'cpage': 1}, timeout=40)
        # 릴레이가 HTML 에러페이지를 중계한 경우 openpyxl 전에 차단 (xlsx 매직넘버 PK)
        if not content or content[:2] != b'PK':
            raise RuntimeError('릴레이 응답이 xlsx가 아님 (매직넘버 PK 불일치)')
        print("[비즈인포] 직접 실패 → 릴레이 성공")
        return content


def _crawl_bizinfo_via_api():
    """엑셀(직접+릴레이) 실패 시 3차 폴백: 기업마당 JSON API → 기존 스키마 매핑.

    BIZINFO_API_KEY와 릴레이 설정이 전제 — API 호스트도 www.bizinfo.go.kr이라
    직접 접근은 동일하게 차단되므로 릴레이 경유(fetch_html의 _maybe_relay)가 필수.
    분야코드 8개를 순회하되 INCLUDE 4분야('금융','기술','수출','창업')는 전량,
    제외 4분야는 P1 회수 게이트(_is_reclaimable_title) 통과분만 수집 — 엑셀 경로와
    필터 정책 일치. 사업개요(bsnsSumryCn)가 응답에 내장돼 상세 fetch가 불필요.
    """
    if not (BIZINFO_API_KEY and BIZINFO_RELAY_BASE):
        return []
    _BIZINFO_STATE['relay'] = True

    def _d(s):
        s = (s or '').strip()
        # 'YYYYMMDD' → 'YYYY-MM-DD'. 비정형 값('예산 소진시까지' 등)은 원문 유지
        # — _load_prev_bizinfo_items의 '파싱 실패=보존'과 동일한 보수 처리.
        return f'{s[:4]}-{s[4:6]}-{s[6:8]}' if len(s) == 8 and s.isdigit() else s

    today = datetime.now().date()
    include_codes = {c for c, name in _BIZINFO_LCLAS.items()
                     if name in BIZINFO_INCLUDE_FIELDS}
    out, seen_links, reclaimed = [], set(), 0
    for code in _BIZINFO_LCLAS:
        raw = fetch_html(BIZINFO_API_URL, timeout=40,
                         params={'crtfcKey': BIZINFO_API_KEY, 'dataType': 'json',
                                 'searchCnt': '0', 'searchLclasId': code})
        d = json.loads(raw)
        # 무키/오류 응답은 HTTP 200 + reqErr 본문 → 상태코드가 아닌 본문 검사
        if isinstance(d, dict) and 'reqErr' in d:
            raise RuntimeError(f"기업마당 API 오류: {d['reqErr']}")
        if isinstance(d, dict):
            items = d.get('jsonArray') or d.get('item') or d.get('items') or []
        else:
            items = d if isinstance(d, list) else []
        for it in items:
            title = str(it.get('pblancNm') or '').strip()
            link = str(it.get('pblancUrl') or '').strip()
            if not title or not link or link in seen_links:
                continue
            if BIZINFO_NOISE_RE.search(title):
                continue
            # 분야 필터: 제외 분야는 P1 회수 게이트 통과분만 (엑셀 경로와 정책 일치)
            if code not in include_codes:
                try:
                    reclaimable = _is_reclaimable_title(title)
                except NameError:  # P1 게이트 미적용(revert) 시: 회수 없이 제외
                    reclaimable = False
                if not reclaimable:
                    continue
                reclaimed += 1
            period_parts = [_d(p) for p in str(it.get('reqstBeginEndDe') or '').split('~')]
            period = ' ~ '.join(p for p in period_parts if p) or '상시'
            # 마감 필터: 종료일 파싱 성공 + 오늘 미만이면 제외 (엑셀 경로 (2)와 동일)
            end_str = period.split('~')[-1].strip()[:10]
            try:
                if datetime.strptime(end_str, '%Y-%m-%d').date() < today:
                    continue
            except Exception:
                pass
            seen_links.add(link)
            out.append({
                '지원사업명': title,
                '신청기간': period,
                '링크': link,  # 절대 www.bizinfo.go.kr URL → first_seen·dedupe·캐시 키 호환
                '사업개요': re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ',
                                                   str(it.get('bsnsSumryCn') or ''))).strip(),
                '등록일자': str(it.get('creatPnttm') or '')[:10],
                '출처': '비즈인포',  # 즉시 태깅 — main()이 enrich를 생략
            })
        time.sleep(0.6)
    print(f"[비즈인포] JSON API 수집 {len(out)}건 (제외분야회수 {reclaimed}건)")
    return out


def crawl_bizinfo_list(driver=None):
    """비즈인포(기업마당) 전체 공고를 엑셀 일괄 다운로드로 수집.

    신 시스템(selectSIIA200) 전환 대응 + '최신 N건만' 누락 문제 해결:
      1) selectSIIA200ExcelDownload.do 로 전체 목록(구조화 데이터) 1회 수신
      2) 신청종료일자 >= 오늘  → 현재 신청 가능한 공고만 (예산소진형 장기공고 포함)
      3) 지원분야 ∈ BIZINFO_INCLUDE_FIELDS → 직접지원성 핵심 분야만
      4) 단순 안내/결과 발표성 제목 제외
    driver 인자는 하위호환용(미사용). 상세 사업개요는 main()에서 requests로 보강.
    """
    import openpyxl  # 무거운 의존성 → 함수 내부 import
    print("비즈인포(기업마당) 엑셀 일괄 수집 시작...")
    try:
        # (1차) 직접 → (2차) 릴레이 재시도 (_bizinfo_excel_bytes 내부에서 처리)
        content = _bizinfo_excel_bytes()
        # read_only=True는 이 xlsx의 dimension 메타데이터 부재로 1행만 인식 → 미사용.
        # 파일이 작아(~110KB) 일반 로드해도 부담 없음.
        wb = openpyxl.load_workbook(io.BytesIO(content), data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        _BIZINFO_STATE['mode'] = 'excel'
    except Exception as e:
        print(f"[비즈인포] 엑셀 수집 실패(직접+릴레이): {e}")
        # (3차) 기업마당 JSON API 폴백 (릴레이 경유, env 미설정 시 빈 결과)
        try:
            api_items = _crawl_bizinfo_via_api()
        except Exception as api_e:
            print(f"[비즈인포] JSON API 폴백 실패: {api_e}")
            api_items = []
        if api_items:
            _BIZINFO_STATE['mode'] = 'api'
            return api_items
        # (4차) 기존 보존 경로 — 현행 로직 무변경
        _BIZINFO_STATE['mode'] = 'preserved'
        print("[비즈인포] 엑셀·API 모두 실패 → 이전 data.json 비즈인포 항목 보존 시도")
        return _load_prev_bizinfo_items()

    if not rows or len(rows) < 2:
        print("[비즈인포] 엑셀에 데이터가 없습니다.")
        return []

    # 헤더 기반 컬럼 매핑 (열 순서 변동 대비)
    header = [str(h).strip() if h else '' for h in rows[0]]
    def col(*names, default=None):
        for n in names:
            if n in header:
                return header.index(n)
        return default
    i_title = col('공고명', '지원사업명', default=4)
    i_field = col('지원분야', '분야', default=3)
    i_start = col('신청시작일자', default=5)
    i_end   = col('신청종료일자', default=6)
    i_url   = col('공고상세URL', 'URL', default=8)
    i_reg   = col('등록일자', default=7)  # 공고 (재)등록일 — 추천 정렬 recency 신호

    today = datetime.now().date()
    all_data = []
    skipped_closed = skipped_field = skipped_noise = 0
    for r in rows[1:]:
        if not r or len(r) <= max(i_title, i_url, i_field, i_end):
            continue
        title = (str(r[i_title]).strip() if r[i_title] else '')
        url   = (str(r[i_url]).strip() if r[i_url] else '')
        field = (str(r[i_field]).strip() if r[i_field] else '')
        if not title or not url:
            continue

        # (2) 현재 신청 가능 필터: 종료일 파싱되면 오늘 이후만, 파싱 실패 시 포함(보수적)
        end_date = None
        try:
            end_date = datetime.strptime(str(r[i_end])[:10], '%Y-%m-%d').date()
        except Exception:
            end_date = None
        if end_date is not None and end_date < today:
            skipped_closed += 1
            continue

        # (3) 핵심 분야 필터
        if BIZINFO_INCLUDE_FIELDS and field not in BIZINFO_INCLUDE_FIELDS:
            skipped_field += 1
            continue

        # (4) 노이즈 제목 제외
        if BIZINFO_NOISE_RE.search(title):
            skipped_noise += 1
            continue

        all_data.append({
            '지원사업명': title,
            '신청기간': _format_period(r[i_start], r[i_end]),
            '링크': url,
            '사업개요': '',
            '등록일자': (str(r[i_reg])[:10] if len(r) > i_reg and r[i_reg] else ''),
        })

    print(f"[비즈인포] 엑셀 {len(rows)-1}건 중 수집 {len(all_data)}건 "
          f"(마감제외 {skipped_closed} / 분야제외 {skipped_field} / 노이즈제외 {skipped_noise})")
    return all_data


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
# ── 추천 정렬 가중치 (index.html의 동일 상수와 1:1 동기화 필수) ───────────
# 기존 5단 사전식 정렬은 상위 동률군에서 최하위 '남은일수 많은순'으로 떨어져
# 연말(12-31) 장기공고를 상단 독식시켰다. 이를 해소하기 위해 (A)최신성·
# (C)기업실익·(B)신청가능성의 가중합 score로 전환한다.
RANK_HALF_LIFE_DAYS = 14    # recency 지수감쇠 반감기(일)
RANK_W_REC = 0.45           # 최신성 가중 (A)
RANK_W_DIR = 0.30           # 직접 재정지원(기업실익) 가중 (C)
RANK_W_APP = 0.25           # 신청가능성(예산잔여 프록시+마감균형) 가중 (B)
RANK_RECENCY_FLOOR = 0.15   # recency 최소값(오래된 공고도 0은 아님)
RANK_APP_NONE = 0.30        # 신청기간 미상 시 applicability
RANK_APP_LONG_TAIL = 0.50   # 마감 >180일(연말 장기공고) 캡 → 쏠림 억제
RANK_TIE_BAND = 0.01        # |Δscore| 이 값 미만이면 동률로 보고 셔플
RANK_DIV_NAME_PREFIX = 16   # 다양성: 동일 사업명 판정용 정규화 prefix 길이
RANK_DIV_MAX_RUN = 2        # 다양성: 같은 출처 연속 최대 허용 개수


def _direct_support_score(name, overview=''):
    """지원사업명+사업개요로 추정한 직접 지원 강도(연속값 0~1.0).
    1.0=제목에 재정/바우처 키워드, 0.85=본문에만(제목 누락분 회수, 신뢰도↓),
    0.5=제목 간접혜택(MID), 0.4=본문 간접, 0.0=불명.
    """
    name = name or ''
    overview = overview or ''
    for kw in DIRECT_SUPPORT_KEYWORDS_HIGH:
        if kw in name:
            return 1.0
    for kw in DIRECT_SUPPORT_KEYWORDS_HIGH:
        if kw in overview:
            return 0.85
    for kw in DIRECT_SUPPORT_KEYWORDS_MID:
        if kw in name:
            return 0.5
    for kw in DIRECT_SUPPORT_KEYWORDS_MID:
        if kw in overview:
            return 0.4
    return 0.0


def _parse_first_date(period):
    """신청기간 첫 날짜(접수 시작일) 파싱. 없으면 None."""
    m = re.search(r'(\d{4})[-./](\d{1,2})[-./](\d{1,2})', period or '')
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
    except ValueError:
        return None


def _parse_last_date(period):
    """신청기간 마지막 날짜(마감일) 파싱. 없으면 None."""
    dates = re.findall(r'(\d{4})[-./](\d{1,2})[-./](\d{1,2})', period or '')
    if not dates:
        return None
    y, m, d = dates[-1]
    try:
        return datetime(int(y), int(m), int(d)).date()
    except ValueError:
        return None


def _recency_score(item, today):
    """최신성(A): 등록일자(비즈인포 공고 (재)등록일)·접수 시작일·first_seen 중
    가장 최근 날짜 기준 지수감쇠 (index.html effectiveAgeDays/recencyScore와 동일).
    등록일자는 비즈인포가 부여하는 실제 등록일. 신규 수집 항목은 first_seen이
    등록일자보다 항상 늦거나 같아(공고는 등록 후 수집됨) 효과가 없고, 기존 공고가
    수정공고로 in-place 재등록될 때(등록일자만 갱신, first_seen은 과거 보존)
    '신선'으로 복권된다 — 효과는 크롤 누적 후 나타나는 관측형 신호.
    미래 날짜는 신선 가산에서 제외하고, 후보가 없으면 30일로 간주.
    """
    cands = []
    start = _parse_first_date(item.get('신청기간') or '')
    if start is not None and start <= today:
        cands.append(start)
    for raw in (item.get('first_seen'), item.get('등록일자')):
        try:
            d = datetime.strptime(str(raw or '')[:10], '%Y-%m-%d').date()
        except (ValueError, TypeError):
            continue
        if d <= today:
            cands.append(d)
    eff_age = 30 if not cands else max(0, (today - max(cands)).days)
    return max(RANK_RECENCY_FLOOR, math.exp(-eff_age / RANK_HALF_LIFE_DAYS))


def _applicability_score(period, today):
    """신청가능성(B): 마감까지 여유로 근사(예산잔여 직접신호 부재).
    plateau형 — 너무 임박/너무 장기 양쪽 감점, >180일은 캡. 만료는 -1.0(최하단 신호).
    """
    end = _parse_last_date(period)
    if end is None:
        return RANK_APP_NONE
    dl = (end - today).days
    if dl < 0:
        return -1.0
    if dl < 3:
        return 0.55
    if dl < 7:
        return 0.80
    if dl <= 45:
        return 1.00
    if dl <= 90:
        return 0.85
    if dl <= 180:
        return 0.65
    return RANK_APP_LONG_TAIL


def _sort_key(item, today=None):
    """추천 정렬 키. 가중합 score 기반(정렬 ascending → score 음수화).
    만료(applicability<0)는 항상 최하단(마감 늦은 순).
    """
    today = today or datetime.now().date()
    period = item.get('신청기간') or ''
    app = _applicability_score(period, today)
    if app < 0:
        end = _parse_last_date(period)
        return (1, -(end.toordinal() if end else 0))
    rec = _recency_score(item, today)
    direct = _direct_support_score(item.get('지원사업명') or '', item.get('사업개요') or '')
    score = RANK_W_REC * rec + RANK_W_DIR * direct + RANK_W_APP * app
    return (0, -score)


def _normalize_name(name):
    """다양성 판정용 사업명 정규화: 선행 [지역]·연도·공백 제거 후 prefix."""
    s = re.sub(r'^\s*[\[\(][^\]\)]*[\]\)]\s*', '', name or '')
    s = re.sub(r'20\d{2}\s*년?', '', s)
    s = re.sub(r'\s+', '', s)
    return s[:RANK_DIV_NAME_PREFIX]


def _diversify(items):
    """score 정렬된 리스트에 다양성 후처리(결정론적, 프론트와 동일 구현):
      (1) 동일 사업명(정규화) 중복은 첫 1개만 상위, 나머지는 후순위로.
      (2) 같은 출처가 연속 RANK_DIV_MAX_RUN 초과하지 않게 그리디 재배치.
    """
    seen, primary, demoted = {}, [], []
    for it in items:
        key = _normalize_name(it.get('지원사업명') or '')
        if key and seen.get(key, 0) >= 1:
            demoted.append(it)
        else:
            seen[key] = seen.get(key, 0) + 1
            primary.append(it)
    spread, pool = [], list(primary)
    while pool:
        placed = False
        for i, it in enumerate(pool):
            src = it.get('출처') or ''
            run = 0
            for prev in reversed(spread):
                if (prev.get('출처') or '') == src:
                    run += 1
                else:
                    break
            if run < RANK_DIV_MAX_RUN:
                spread.append(pool.pop(i))
                placed = True
                break
        if not placed:
            spread.append(pool.pop(0))
    return spread + demoted

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
    _today = datetime.now().date()
    # 프론트 getRecommended와 동일 구조: 만료/생존 분리 → 생존만 다양성 후처리 →
    # 만료(마감 늦은 순)를 최하단에 부착. 만료 포함 전체에 _diversify를 적용하면
    # demote된 생존 중복이 만료 아래로 가라앉아 화면(프론트)·디스크 순서가 어긋난다.
    _alive, _expired = [], []
    for _it in data:
        if _applicability_score(_it.get('신청기간') or '', _today) < 0:
            _expired.append(_it)
        else:
            _alive.append(_it)
    _alive.sort(key=lambda it: _sort_key(it, _today))
    _expired.sort(key=lambda it: _sort_key(it, _today))
    sorted_data = _diversify(_alive) + _expired

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
            html = fetch_html(base_url, params=params, timeout=20)
            soup = BeautifulSoup(html, 'html.parser')
            
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
            html = fetch_html(base_url, params=params, timeout=20)
            soup = BeautifulSoup(html, 'html.parser')

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
            html = fetch_html(link, timeout=10)
            soup = BeautifulSoup(html, 'html.parser')
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
            html = fetch_html(base_url, params=params, timeout=20)
            soup = BeautifulSoup(html, 'html.parser')
            
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
            html = fetch_html(base_url, params=params, headers=headers, timeout=20)
            soup = BeautifulSoup(html, 'html.parser')
            
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
            html = fetch_html(base_url, params=params, headers=headers, timeout=20)
            soup = BeautifulSoup(html, 'html.parser')

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
            html = fetch_html(base_url, params=params, headers=headers, timeout=20)
            soup = BeautifulSoup(html, 'html.parser')
            
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
            html = fetch_html(item['링크'], headers=_hdrs, timeout=10)
            soup = BeautifulSoup(html, 'html.parser')

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

            html = fetch_html(url, headers=headers, timeout=20)
            soup = BeautifulSoup(html, 'html.parser')
            
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
            html = fetch_html(item['\ub9c1\ud06c'], headers=_hdrs, timeout=10)
            soup = BeautifulSoup(html, 'html.parser')
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
            # CBTP는 DESAdapter(레거시 SSL) + EUC-KR 강제. session 명시 시 fetch_html은
            # Scrapling을 건너뛰고 이 session으로만 요청 → DH_KEY_TOO_SMALL 회피.
            html = fetch_html(url, session=session, headers=headers, encoding='EUC-KR', timeout=20)
            soup = BeautifulSoup(html, 'html.parser')
            
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
    충북테크노파크 상세 크롤링 — DESAdapter 세션으로 enrich_detail 호출.
    레거시 SSL(DH_KEY_TOO_SMALL) 때문에 GLOBAL_SESSION으로는 접속 불가.
    """
    if not data_list:
        return data_list
    cbtp_session = requests.Session()
    cbtp_session.verify = False
    cbtp_session.headers.update(GLOBAL_SESSION.headers)
    cbtp_session.mount('https://', DESAdapter())

    ctx = {'session': cbtp_session, 'base_url': 'https://www.cbtp.or.kr'}

    def _one(it):
        try:
            return enrich_detail(it, ctx=ctx)
        except Exception:
            return it

    with ThreadPoolExecutor(max_workers=3) as ex:
        return list(ex.map(_one, data_list))



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

            try:
                html = fetch_html(url, timeout=10)
            except Exception as e:
                print(f"    [오류] 페이지 로드 실패: {e}")
                continue
            soup = BeautifulSoup(html, 'html.parser')
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

            try:
                html = fetch_html(url, headers=headers, timeout=10)
            except Exception as e:
                print(f"    [오류] 페이지 로드 실패: {e}")
                continue
            soup = BeautifulSoup(html, 'html.parser')
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
            try:
                html = fetch_html(item['링크'], headers=_hdrs, timeout=10)
            except Exception:
                return item
            soup = BeautifulSoup(html, 'html.parser')
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

            try:
                html = fetch_html(url, headers=headers, timeout=10)
            except Exception as e:
                print(f"    [오류] 페이지 로드 실패: {e}")
                continue
            soup = BeautifulSoup(html, 'html.parser')
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

            try:
                html = fetch_html(url, headers=headers, timeout=10)
            except Exception as e:
                print(f"    [오류] 페이지 로드 실패: {e}")
                continue
            soup = BeautifulSoup(html, 'html.parser')
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

            try:
                html = fetch_html(url, headers=headers, timeout=10)
            except Exception as e:
                print(f"    [오류] 페이지 로드 실패: {e}")
                continue
            soup = BeautifulSoup(html, 'html.parser')
            
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
            # CTP는 일반 requests로 가능 (SSL verify=False) — fetch_html이 Scrapling 우선 시도
            html = fetch_html(url, headers=headers, timeout=20)
            soup = BeautifulSoup(html, 'html.parser')
            
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
        time.sleep(1.0)  # SPA 렌더 대기 (기존 0.5는 너무 짧음)

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

        # Case 4: 일반 HTML 키워드 추출 (렌더된 DOM 통째로)
        if not content or len(content) < 30:
            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                generic = extract_summary_from_html(soup)
                if generic and len(generic) > 30:
                    content = generic
            except:
                pass

        # Case 5: 첨부 PDF/HWPX 본문 추출 fallback
        if not content or len(content) < 30:
            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                base_url = '/'.join(link.split('/')[:3]) if link.startswith('http') else 'https://www.sbiz24.kr'
                for fname, furl, ext in find_attachment_links(soup, base_url=base_url):
                    txt = ""
                    if ext == 'pdf':
                        txt = extract_text_from_pdf(furl)
                    elif ext == 'hwpx':
                        txt = extract_text_from_hwpx(furl)
                    if txt and len(txt) > 100:
                        content = summarize_long_text(txt, max_len=500)
                        break
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
    bizinfo_preserved = False  # 예외 시 NameError 방지 — try 내부에서 재할당


    # 1. 기업마당 (BizInfo) — 엑셀 일괄 수집 + requests 점진적 상세 보강 (Selenium 미사용)
    try:
        bizinfo_list = crawl_bizinfo_list()  # 엑셀 다운로드 → 신청가능+핵심분야 필터
        # 분기 식별은 _BIZINFO_STATE['mode'] 기반(excel|api|preserved).
        # mode가 None인 예외 상황만 기존 출처 휴리스틱 폴백: 보존/API 항목은 이미
        # '출처'='비즈인포'로 태깅돼 있고 정상 엑셀 수집 항목엔 '출처'가 아직 없다.
        bizinfo_mode = _BIZINFO_STATE.get('mode')
        if bizinfo_mode is None and bizinfo_list:
            bizinfo_mode = 'preserved' if all(
                it.get('출처') == '비즈인포' for it in bizinfo_list) else 'excel'
        bizinfo_preserved = (bizinfo_mode == 'preserved')
        if bizinfo_preserved:
            # 보존 항목을 enrich에 다시 보내면 차단된 bizinfo.go.kr에 재접속해
            # 무의미한 timeout이 누적되므로 상세 보강을 건너뛴다.
            final_results.extend(bizinfo_list)
            print(f"[비즈인포] 엑셀 수집 실패 → 이전 데이터 {len(bizinfo_list)}건 보존 (상세 보강 생략)")
        elif bizinfo_mode == 'api':
            # JSON API 모드: 사업개요(bsnsSumryCn) 내장 + 출처 태깅 완료 → enrich 생략
            # (상세 fetch 수백 회 절감).
            final_results.extend(bizinfo_list)
            print(f"[비즈인포] JSON API 모드 {len(bizinfo_list)}건 수집 (사업개요 내장 — 상세 보강 생략)")
        elif bizinfo_list:
            # 점진적 보강: 기존 data.json의 사업개요를 미리 채워두면
            # enrich_detail()이 '≥80자면 skip' 규칙으로 신규 공고만 fetch 한다.
            summary_cache = _load_existing_summaries()
            cached_hits = 0
            for it in bizinfo_list:
                c = summary_cache.get((it.get('링크') or '').strip())
                if c:
                    it['사업개요'] = c
                    cached_hits += 1
            print(f"[비즈인포] 사업개요 캐시 재사용 {cached_hits}건 / 신규 fetch 대상 "
                  f"{len(bizinfo_list) - cached_hits}건")
            # 신 상세페이지는 정적 HTML → requests 병렬로 사업개요 보강 (Selenium 불필요)
            bizinfo_list = crawl_generic_details(
                bizinfo_list, max_workers=PHASE2_WORKERS,
                ctx={'html_processor': _bizinfo_extract_summary})
            for item in bizinfo_list:
                item['출처'] = '비즈인포'
            final_results.extend(bizinfo_list)
            print(f"[비즈인포] 최종 {len(bizinfo_list)}건 수집 완료")
    except Exception as e:
        print(f"[오류] 기업마당 크롤링 실패: {e}")

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
             # 상세 페이지(`intro.asp?seq=N`)는 정적 HTML — ITP의 fncFileDownload
             # JS 콜백은 attachment_resolver로 다운로드 URL 후보를 추정
             itp_ctx = {'attachment_resolver': _itp_attachment_resolver}
             itp_results = crawl_generic_details(itp_results, max_workers=4, ctx=itp_ctx)
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
        """requests 기반 크롤러 실행 래퍼 (list + optional site-specific detail + generic enrichment)"""
        try:
            items = fn_list(end_page=end_page) if end_page else fn_list()
            if items and fn_detail:
                items = fn_detail(items)
            # 사이트별 detail 크롤러로 사업개요를 못 채웠거나 짧은 항목은
            # 일반 enrichment(HTML 키워드 + 첨부 PDF/HWPX 추출)로 보강.
            # enrich_detail() 안에서 사업개요 ≥80자면 자동 skip.
            if items:
                items = crawl_generic_details(items, max_workers=3)
            return items or []
        except Exception as e:
            print(f"[오류] {name} 크롤링 실패: {e}")
            return []

    print(f"\n[병렬] requests 기반 크롤러 동시 실행 시작... (workers={PHASE2_WORKERS})")
    with ThreadPoolExecutor(max_workers=PHASE2_WORKERS) as ex:
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
            # JTP 상세도 SPA — 동일 driver로 순차 enrichment
            print(f"[제주테크노파크] {len(jtp_list)}개 항목 상세 수집 시작...")
            ctx = {'driver': driver, 'spa_wait': 2.0}
            jtp_list = [enrich_detail(it, ctx=ctx) for it in jtp_list]
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

    # 핵심정보(첨부 공고문 구조화 추출) — 별도 pass, 실패해도 기존 카드 무손상.
    # bizinfo_ok는 mode 기반: excel(직접/릴레이)·api(릴레이) 모드면 첨부 다운로드도
    # _maybe_relay로 우회 가능하므로 활성. preserved(전면 실패)일 때만 비활성.
    try:
        extract_key_info_pass(
            final_results,
            bizinfo_ok=(_BIZINFO_STATE.get('mode') in ('excel', 'api')))
    except Exception as e:
        print(f"[핵심정보] 추출 pass 실패(격리됨, 기존 카드 무손상): {e}")

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
