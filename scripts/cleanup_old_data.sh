#!/usr/bin/env bash
set -euo pipefail

# 오래된 데이터 정리 스크립트
# 7일 이상 된 데이터를 자동으로 삭제하여 저장 공간 확보

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${REPO_ROOT}/project/logs"
CLEANUP_LOG="${LOG_DIR}/cleanup_$(date '+%Y%m%d').log"

echo "============================"
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Data Cleanup Starting"
echo "============================"

# 7일 이상 된 Landing 데이터 삭제
echo "Cleaning up old landing data (older than 7 days)..."
find "${REPO_ROOT}/project/data/landing" -name "events_*.json" -mtime +7 -delete 2>/dev/null || true

# 14일 이상 된 Bronze 데이터 삭제 (Delta Lake는 자동 정리)
echo "Cleaning up old bronze data (older than 14 days)..."
find "${REPO_ROOT}/project/data/bronze" -name "*.parquet" -mtime +14 -delete 2>/dev/null || true

# 30일 이상 된 Silver 데이터 삭제
echo "Cleaning up old silver data (older than 30 days)..."
find "${REPO_ROOT}/project/data/silver" -name "*.parquet" -mtime +30 -delete 2>/dev/null || true

# 60일 이상 된 Gold 데이터 삭제
echo "Cleaning up old gold data (older than 60 days)..."
find "${REPO_ROOT}/project/data/gold" -name "*.parquet" -mtime +60 -delete 2>/dev/null || true

# 7일 이상 된 로그 파일 삭제
echo "Cleaning up old log files (older than 7 days)..."
find "${LOG_DIR}" -name "*.log" -mtime +7 -delete 2>/dev/null || true

# Spark 임시 파일 정리
echo "Cleaning up Spark temporary files..."
find "${REPO_ROOT}/project/tmp" -name "*" -mtime +1 -delete 2>/dev/null || true

# 압축 파일 생성 (공간 절약)
echo "Compressing old data files..."
find "${REPO_ROOT}/project/data" -name "*.json" -mtime +3 -exec gzip {} \; 2>/dev/null || true

echo "============================"
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Data Cleanup Completed"
echo "============================"
