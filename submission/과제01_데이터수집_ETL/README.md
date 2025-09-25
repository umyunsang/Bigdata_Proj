# Assignment 01 · Data Ingestion & Bronze ETL

This folder documents the YouTube Data API v3 ingestion flow and the landing→bronze promotion carried out in the snippet project.

## Screenshots
- `screenshots/Fetch 실행 결과.png`: Terminal capture of the Step 01 fetch command showing successful retrieval of YouTube metadata (or mock fallback) and the generated landing/sample file paths.
- `screenshots/NDJSON 샘플 데이터.png`: Preview of the reservoir-sampled NDJSON payload written after ingestion, illustrating the raw schema captured from the API.
- `screenshots/수집된 파일 확인.png`: File browser view confirming that new `events_*.json` assets were created in the landing directory for the current run.
- `screenshots/Bronze 실행 결과.png`: CLI output from the bronze batch job (Spark + Delta) highlighting Bloom-filter prechecks and append counts.
- `screenshots/Bronze 데이터 확인.png`: Data inspection of the bronze Delta table verifying that normalized columns (post_id, video_id, ts, ingest_date, source) were written.
- `screenshots/Delta Lake 로그.png`: Spark/Delta Lake log excerpt demonstrating checkpointing and transaction metadata for the bronze write.
