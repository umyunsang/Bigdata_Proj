## 과제04 — 다모델 학습 & Pareto Front

프로젝트 CLI(추천)
- 학습/파레토: `python -m video_social_rtp.cli train`
- (샌드박스/Spark 불가 시) `python -m video_social_rtp.cli train --fallback-local`
- MLflow 비활성화: `--no-mlflow` (기본은 설치 시 파일기반 로깅 사용)

입력 경로
- Gold 피처: `project/data/gold/features` (Delta; 폴백 시 `project/data/gold/features.csv`)

출력 산출물
- Pareto 결과: `project/artifacts/pareto.json` (모든 후보/전선 포함)
- (MLflow 사용 시) `project/artifacts/mlruns/` 하위에 실험 로그 저장

권장 스크린샷 체크리스트
- 학습 실행 콘솔 로그
- pareto.json 내용 일부(후보 성능표 + Pareto front)
- (선택) MLflow 실험 디렉토리 구조

스니펫 경로(참고)
- `python video-social-rtp-snippets/jobs/40_train_pareto.py`

