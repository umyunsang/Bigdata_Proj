#!/usr/bin/env bash
set -euo pipefail

# 매일 자동 데이터 축적 및 분석 스크립트
# 30일간 데이터 축적: 2025-09-30 ~ 2025-10-30
# SSH 연결이 끊어져도 백그라운드에서 계속 실행되도록 nohup 사용

# 로그 설정
LOG_DIR="/home/student_15030/Bigdata_Proj/project/logs"
mkdir -p "${LOG_DIR}"
DAILY_LOG="${LOG_DIR}/daily_automation_$(date '+%Y%m%d').log"
exec > >(tee -a "${DAILY_LOG}") 2>&1

echo "============================"
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Daily Automation Starting"
echo "============================"

# 프로젝트 디렉토리로 이동
cd /home/student_15030/Bigdata_Proj

# 가상환경 활성화
source bigdata_env/bin/activate

# 환경변수 설정
export VENV_PATH="/home/student_15030/Bigdata_Proj/bigdata_env"
export PIPELINE_BRONZE_FALLBACK_LOCAL=false
export PIPELINE_SILVER_FALLBACK_LOCAL=false
export PIPELINE_GOLD_FALLBACK_LOCAL=false
export PIPELINE_TRAIN_FALLBACK_LOCAL=false
export PIPELINE_ENABLE_MLFLOW=0
export PIPELINE_SKIP_IF_EXISTS=false

# YouTube API 일일 할당량 고려 (10,000 units/day)
# 각 검색당 약 100 units 소모, 하루 최대 100회 검색 가능
# 빅데이터 규모 확보를 위해 하루 80회 검색으로 확장
MAX_DAILY_SEARCHES=80

# 검색어 목록 (K-POP 관련 다양한 키워드)
SEARCH_QUERIES=(
    "K-POP"
    "NewJeans"
    "BLACKPINK"
    "BTS"
    "aespa"
    "LE SSERAFIM"
    "ITZY"
    "TWICE"
    "Stray Kids"
    "SEVENTEEN"
    "IVE"
    "NMIXX"
    "Kep1er"
    "STRAY KIDS"
    "Red Velvet"
    "Girls Generation"
    "2NE1"
    "BIGBANG"
    "EXO"
    "SHINee"
    "K-POP dance"
    "K-POP cover"
    "K-POP reaction"
    "K-POP music"
    "Korean pop"
    "K-pop MV"
    "K-pop live"
    "K-pop performance"
    "Korean music"
    "K-pop choreography"
    "K-pop dance cover"
    "K-pop reaction"
    "K-pop compilation"
    "K-pop mix"
    "K-pop playlist"
    "K-pop hits"
    "K-pop 2024"
    "K-pop 2025"
    "Korean idol"
    "K-pop group"
    "K-pop solo"
    "K-pop debut"
    "K-pop comeback"
    "K-pop stage"
    "K-pop concert"
    "K-pop fan"
    "K-pop culture"
    "K-pop trend"
    "K-pop viral"
    "K-pop challenge"
)

# 랜덤 검색어 선택 (저장 공간 고려하여 하루 10개)
SELECTED_QUERIES=()
for i in {1..10}; do
    RANDOM_INDEX=$((RANDOM % ${#SEARCH_QUERIES[@]}))
    SELECTED_QUERIES+=("${SEARCH_QUERIES[$RANDOM_INDEX]}")
done

echo "Selected queries for today: ${SELECTED_QUERIES[*]}"

# 각 검색어에 대해 데이터 수집 및 처리
for query in "${SELECTED_QUERIES[@]}"; do
    echo "Processing query: $query"
    
    # 파이프라인 실행
    export PIPELINE_QUERY="$query"
    export PIPELINE_MAX_ITEMS=50  # 각 검색당 50개 비디오 (저장 공간 고려)
    export PIPELINE_REGION_CODE="KR"
    export PIPELINE_RELEVANCE_LANGUAGE="ko"
    
    # 파이프라인 실행
    ./scripts/daily_pipeline.sh
    
    echo "Completed processing for: $query"
    sleep 2  # API 호출 간격 조절
done

# 일일 분석 결과 생성
echo "Generating daily analysis report..."
python3 -c "
import sys
sys.path.append('/home/student_15030/Bigdata_Proj')
import json
import pandas as pd
from datetime import datetime
import os

# 데이터 통계 수집
landing_files = [f for f in os.listdir('project/data/landing/') if f.startswith('events_')]
bronze_files = [f for f in os.listdir('project/data/bronze/_delta_log/') if f.endswith('.json')]
gold_file = 'project/data/gold/features.csv'

# 일일 리포트 생성
report = {
    'date': datetime.now().strftime('%Y-%m-%d'),
    'landing_files_count': len(landing_files),
    'bronze_transactions': len(bronze_files),
    'gold_features': 0,
    'queries_processed': len('${SELECTED_QUERIES[@]}'.split()),
    'api_usage_estimate': len('${SELECTED_QUERIES[@]}'.split()) * 10 * 100  # queries * items * units_per_item
}

if os.path.exists(gold_file):
    with open(gold_file, 'r') as f:
        report['gold_features'] = len(f.readlines()) - 1  # 헤더 제외

# 리포트 저장
with open('project/logs/daily_report_$(date +%Y%m%d).json', 'w') as f:
    json.dump(report, f, indent=2)

print(f'Daily report generated: {report}')
"

echo "============================"
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Daily Automation Completed"
echo "============================"
