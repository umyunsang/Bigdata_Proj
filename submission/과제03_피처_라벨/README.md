# Assignment 03 · Feature Engineering & Labeling (Gold Layer)

This folder captures the gold-layer transformation where engagement features are computed and percentile-based labels are assigned from the silver metrics.

## Screenshots
- `screenshots/Gold 실행 결과.png`: CLI trace of the gold feature job, including summaries of rows processed and fallback notices when applicable.
- `screenshots/Gold 데이터 디렉토리.png`: Directory listing that verifies the written Delta/Pandas outputs for gold features.
- `screenshots/Delta Lake 로그.png`: Delta Lake log details for the gold write, showing versioned commits and schema metadata.
- `screenshots/피처 데이터 샘플.png`: Snapshot of the engineered features (e.g., engagement_24h, uniq_users_est) generated from aggregated counts.
- `screenshots/라벨링 결과 통계.png`: Analytical view summarizing label distribution after applying the CDF cut-off (top percentile threshold).
- `screenshots/CDF 컷오프 아티팩트.png`: Artifact preview documenting the persisted percentile cut-off (`gold_cutoff.json`) for reproducibility across downstream steps.
