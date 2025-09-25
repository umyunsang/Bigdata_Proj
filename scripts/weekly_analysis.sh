#!/usr/bin/env bash
set -euo pipefail

# 주간 분석 및 보고서 생성 스크립트
# 매주 일요일에 실행되어 지난 주 데이터 분석 및 보고서 생성

# 로그 설정
LOG_DIR="/home/student_15030/Bigdata_Proj/project/logs"
mkdir -p "${LOG_DIR}"
WEEKLY_LOG="${LOG_DIR}/weekly_analysis_$(date '+%Y%m%d').log"
exec > >(tee -a "${WEEKLY_LOG}") 2>&1

echo "============================"
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Weekly Analysis Starting"
echo "============================"

# 프로젝트 디렉토리로 이동
cd /home/student_15030/Bigdata_Proj

# 가상환경 활성화
source bigdata_env/bin/activate

# 주간 분석 실행
python3 -c "
import sys
sys.path.append('/home/student_15030/Bigdata_Proj')
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import glob

print('=== 주간 데이터 분석 시작 ===')

# 1. 데이터 수집 통계
landing_files = glob.glob('project/data/landing/events_*.json')
bronze_transactions = glob.glob('project/data/bronze/_delta_log/*.json')
gold_file = 'project/data/gold/features.csv'

# 2. 피처 데이터 분석
if os.path.exists(gold_file):
    df = pd.read_csv(gold_file)
    
    # 기본 통계
    stats = {
        'total_videos': len(df),
        'avg_engagement': df['engagement_24h'].mean(),
        'max_engagement': df['engagement_24h'].max(),
        'avg_unique_users': df['uniq_users_est'].mean(),
        'max_unique_users': df['uniq_users_est'].max(),
        'high_engagement_videos': len(df[df['engagement_24h'] > df['engagement_24h'].quantile(0.9)]),
        'viral_videos': len(df[df['engagement_24h'] > df['engagement_24h'].quantile(0.95)])
    }
    
    # 상위 비디오 분석
    top_videos = df.nlargest(10, 'engagement_24h')[['video_id', 'engagement_24h', 'uniq_users_est']]
    
    print(f'총 비디오 수: {stats[\"total_videos\"]}')
    print(f'평균 참여도: {stats[\"avg_engagement\"]:.2f}')
    print(f'최대 참여도: {stats[\"max_engagement\"]}')
    print(f'고참여 비디오: {stats[\"high_engagement_videos\"]}개')
    print(f'바이럴 비디오: {stats[\"viral_videos\"]}개')
    
    # 3. 주간 보고서 생성
    weekly_report = {
        'week_ending': datetime.now().strftime('%Y-%m-%d'),
        'data_collection': {
            'landing_files': len(landing_files),
            'bronze_transactions': len(bronze_transactions),
            'total_videos_processed': stats['total_videos']
        },
        'engagement_analysis': {
            'average_engagement': round(stats['avg_engagement'], 2),
            'max_engagement': int(stats['max_engagement']),
            'high_engagement_count': stats['high_engagement_videos'],
            'viral_count': stats['viral_videos']
        },
        'top_performers': top_videos.to_dict('records'),
        'insights': [
            f'이번 주 총 {stats[\"total_videos\"]}개 비디오 분석',
            f'평균 참여도 {stats[\"avg_engagement\"]:.1f}회',
            f'{stats[\"viral_videos\"]}개 바이럴 비디오 발견',
            f'최고 참여도 {stats[\"max_engagement\"]}회 달성'
        ]
    }
    
    # 보고서 저장
    report_file = f'project/logs/weekly_report_{datetime.now().strftime(\"%Y%m%d\")}.json'
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(weekly_report, f, ensure_ascii=False, indent=2)
    
    print(f'주간 보고서 생성: {report_file}')
    
else:
    print('Gold 데이터 파일이 없습니다.')

print('=== 주간 분석 완료 ===')
"

echo "============================"
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Weekly Analysis Completed"
echo "============================"
