#!/usr/bin/env bash
set -euo pipefail

# Cron 작업 설정 스크립트
# 매일 자동 데이터 축적 및 주간 분석 스케줄링

echo "=== Cron 작업 설정 시작 ==="

# 현재 사용자의 cron 작업 확인
echo "현재 cron 작업:"
crontab -l 2>/dev/null || echo "현재 cron 작업이 없습니다."

# 새로운 cron 작업 설정
CRON_JOBS="
# 매일 오전 9시에 데이터 축적 실행
0 9 * * * /home/student_15030/Bigdata_Proj/scripts/daily_automation.sh

# 매주 일요일 오전 10시에 주간 분석 실행
0 10 * * 0 /home/student_15030/Bigdata_Proj/scripts/weekly_analysis.sh

# 매일 오후 6시에 데이터 백업 (선택사항)
0 18 * * * cd /home/student_15030/Bigdata_Proj && tar -czf project/backup_$(date +\%Y\%m\%d).tar.gz project/data/ project/logs/ --exclude='project/data/landing/events_*.json' 2>/dev/null || true
"

# 기존 cron 작업에 추가
(crontab -l 2>/dev/null; echo "$CRON_JOBS") | crontab -

echo "=== Cron 작업 설정 완료 ==="
echo "설정된 작업:"
echo "1. 매일 오전 9시: 데이터 축적"
echo "2. 매주 일요일 오전 10시: 주간 분석"
echo "3. 매일 오후 6시: 데이터 백업"

# 설정 확인
echo ""
echo "현재 cron 작업 목록:"
crontab -l
