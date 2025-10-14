#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  local exit_code=$?
  local ts
  ts="$(date '+%Y-%m-%d %H:%M:%S%z')"
  if (( exit_code == 0 )); then
    echo "[${ts}] pipeline completed successfully"
  else
    echo "[${ts}] pipeline failed with exit code ${exit_code}"
  fi
}
trap cleanup EXIT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

LOG_DIR="${PIPELINE_LOG_DIR:-${REPO_ROOT}/project/logs}"
mkdir -p "${LOG_DIR}"
RUN_LOG="${RUN_LOG:-${LOG_DIR}/daily_pipeline_$(date '+%Y%m%d').log}"
touch "${RUN_LOG}"
exec > >(tee -a "${RUN_LOG}") 2>&1

echo "============================"
echo "[$(date '+%Y-%m-%d %H:%M:%S%z')] daily pipeline starting"
echo "repo_root=${REPO_ROOT}"
echo "log_file=${RUN_LOG}"
echo "============================"

VENV_PATH="${VENV_PATH:-${REPO_ROOT}/.venv}"
if [[ ! -f "${VENV_PATH}/bin/activate" ]]; then
  echo "virtualenv not found at ${VENV_PATH}. create it via 'python -m venv .venv' and install dependencies." >&2
  exit 1
fi
# shellcheck source=/dev/null
source "${VENV_PATH}/bin/activate"

PYTHON_BIN="${PYTHON_BIN:-python}"

if [[ -z "${YT_API_KEY:-}" && -z "${YOUTUBE_API_KEY:-}" ]]; then
  echo "YouTube API key not found (set YT_API_KEY or YOUTUBE_API_KEY). Real-data pipeline requires API access." >&2
  exit 1
fi

QUERY="${PIPELINE_QUERY:-data engineering}"
MAX_ITEMS="${PIPELINE_MAX_ITEMS:-50}"
RESERVOIR_K="${PIPELINE_RESERVOIR_K:-64}"
REGION="${PIPELINE_REGION_CODE:-}"
RELEVANCE="${PIPELINE_RELEVANCE_LANGUAGE:-}"
SKIP_IF_EXISTS="${PIPELINE_SKIP_IF_EXISTS:-true}"
FORCE_MOCK="${PIPELINE_FORCE_MOCK:-false}"
TOP_PCT="${PIPELINE_TOP_PCT:-0.9}"
BRONZE_FALLBACK="${PIPELINE_BRONZE_FALLBACK_LOCAL:-false}"
SILVER_FALLBACK="${PIPELINE_SILVER_FALLBACK_LOCAL:-false}"
GOLD_FALLBACK="${PIPELINE_GOLD_FALLBACK_LOCAL:-false}"
TRAIN_FALLBACK="${PIPELINE_TRAIN_FALLBACK_LOCAL:-false}"
ENABLE_MLFLOW="${PIPELINE_ENABLE_MLFLOW:-0}"
SILVER_ONCE="${PIPELINE_SILVER_ONCE:-true}"
WATERMARK="${PIPELINE_SILVER_WATERMARK:-10 minutes}"
WINDOW_SIZE="${PIPELINE_SILVER_WINDOW:-1 hour}"
WINDOW_SLIDE="${PIPELINE_SILVER_SLIDE:-5 minutes}"

FETCH_ARGS=(-m video_social_rtp.cli fetch --query "${QUERY}" --max-items "${MAX_ITEMS}" --reservoir-k "${RESERVOIR_K}")
if [[ "${SKIP_IF_EXISTS}" == "false" ]]; then
  FETCH_ARGS+=(--no-skip-if-exists)
fi
if [[ -n "${REGION}" ]]; then
  FETCH_ARGS+=(--region-code "${REGION}")
fi
if [[ -n "${RELEVANCE}" ]]; then
  FETCH_ARGS+=(--relevance-language "${RELEVANCE}")
fi
if [[ "${FORCE_MOCK}" == "true" ]]; then
  FETCH_ARGS+=(--mock)
else
  FETCH_ARGS+=(--no-mock)
fi

BRONZE_ARGS=(-m video_social_rtp.cli bronze)
if [[ "${BRONZE_FALLBACK}" == "true" ]]; then
  BRONZE_ARGS+=(--fallback-local)
elif [[ "${BRONZE_FALLBACK}" == "false" ]]; then
  BRONZE_ARGS+=(--no-fallback-local)
fi

SILVER_ARGS=(-m video_social_rtp.cli silver --watermark "${WATERMARK}" --window "${WINDOW_SIZE}" --slide "${WINDOW_SLIDE}")
if [[ "${SILVER_ONCE}" == "false" ]]; then
  SILVER_ARGS+=(--no-once)
else
  SILVER_ARGS+=(--once)
fi
if [[ "${SILVER_FALLBACK}" == "true" ]]; then
  SILVER_ARGS+=(--fallback-local)
elif [[ "${SILVER_FALLBACK}" == "false" ]]; then
  SILVER_ARGS+=(--no-fallback-local)
fi

GOLD_ARGS=(-m video_social_rtp.cli gold --top-pct "${TOP_PCT}")
if [[ "${GOLD_FALLBACK}" == "true" ]]; then
  GOLD_ARGS+=(--fallback-local)
elif [[ "${GOLD_FALLBACK}" == "false" ]]; then
  GOLD_ARGS+=(--no-fallback-local)
fi

TRAIN_ARGS=(-m video_social_rtp.cli train)
if [[ "${ENABLE_MLFLOW}" == "0" ]]; then
  TRAIN_ARGS+=(--no-mlflow)
fi
if [[ "${TRAIN_FALLBACK}" == "true" ]]; then
  TRAIN_ARGS+=(--fallback-local)
elif [[ "${TRAIN_FALLBACK}" == "false" ]]; then
  TRAIN_ARGS+=(--no-fallback-local)
fi

run_step() {
  local name="$1"
  shift
  local start_ts
  start_ts="$(date '+%Y-%m-%d %H:%M:%S%z')"
  echo "[${start_ts}] ${name} start"
  "${PYTHON_BIN}" "$@"
  local end_ts
  end_ts="$(date '+%Y-%m-%d %H:%M:%S%z')"
  echo "[${end_ts}] ${name} done"
}

run_step "fetch" "${FETCH_ARGS[@]}"
run_step "bronze" "${BRONZE_ARGS[@]}"
run_step "silver" "${SILVER_ARGS[@]}"
run_step "gold" "${GOLD_ARGS[@]}"
run_step "train" "${TRAIN_ARGS[@]}"
