# Assignment 02 · Streaming Refinement (Silver Layer)

Screenshots in this directory summarize the Structured Streaming job that aggregates landing events into the silver layer with watermarking and Delta persistence.

## Screenshots
- `screenshots/Silver 실행 결과.png`: Spark streaming console output showing the silver job startup, micro-batch progress, and trigger completion in once-mode.
- `screenshots/워터마크 설정 확인.png`: Code/log snippet validating that event-time watermarking (e.g., `10 minutes`) is active to handle late data.
- `screenshots/처리된 데이터 샘플.png`: Tabular preview of aggregated counts per video ID produced by the streaming window.
- `screenshots/Silver 데이터 디렉토리.png`: File system view confirming the creation of Delta checkpoint and table directories under the configured silver path.
- `screenshots/Delta Lake 로그.png`: Delta transaction log excerpt documenting append commits and schema information for the silver table.
