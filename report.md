# 개별과제: 빅데이터 분석

## 과제 안내

### 구성
- **총 5개 과제(1–5)**
- **마감:** 2025-12-19(금) 23:59, Asia/Seoul — 이후 제출/이용 불가
- **총점:** 1000점

### 제출물
**"완전한 코드 + 실행 스크린샷(각 과제별) + 보고서(PDF)"** 모두 포함

- **video-social-rtp-snippets.zip**
- **00 개별과제.pptx** 문서 미리 보기

### 보고서(PDF, 5쪽이내 권장)
1. **문제 정의·목표**
2. **설계/알고리즘** (선택 이유, 대안 비교)
3. **구현** (구조도/흐름도, 주요 코드 설명)
4. **실험/결과** (표·그래프, 재현 절차)
5. **고찰** (한계·개선안)
6. **참고문헌·라이선스**

### 제출 유의사항

- **마감 시각:** 2025-12-19 23:59 이후 자동 차단(연장/지연 제출 불가)
- **표절/AI 활용:** 아이디어 보조로서의 AI 활용은 가능하나, 코드와 주석은 필수이며, 사용 내역·프롬프트를 보고서 부록에 명시
- **문의 시 포함:** 학번·이름, 오류 로그, 재현 단계, OS/언어·버전

---

## 과제 목표

- 다양한 데이터 소스(비디오, 소셜미디어 스트림 등)로부터 **실시간 데이터 처리** 기법을 단계별로 학습합니다.
- **수집 → 스트리밍 ETL → 피처/라벨 → 모델 선정 → 실시간 서비스**에 대한 이해를 목표로 합니다.
- 개별 코드 스니펫을 제공합니다.

---

## 1과제. 멀티 소스 수집 & 배치 ETL (Bronze)

- **핵심 목표:**  
  - API를 통해 데이터를 수집하고, 원본 보존(스키마 고정, 증분 수집)을 유지합니다.
  - 희소 구간 또는 데이터 폭증 구간에 대응하기 위해 **Reservoir Sampling**을 적용합니다.

- **삽입 포인트 A — Reservoir Sampling (스트리밍 전 단계의 대표 샘플 확보)**  
  - **목적:** API 폭증 시 전수 저장 전 점검/데이터 프로파일링용 대표 샘플을 저비용으로 유지  
  - **방법:** 각 쿼리/키워드별로 크기 k의 저장소를 유지 (Vitter’s Algorithm R 사용)

- **삽입 포인트 B — Bloom Filter(원본 중복 방지의 1차 게이트)**  
  - **목적:** API가 중복 video_id/post_id를 자주 반환하는 경우, 저장/조인 비용을 절감  
  - **방법:** 최근 7일의 ID를 집계해 Bloom filter를 생성 → 인입 시 might_contain으로 빠른 배제

---

## 2과제. 구조화 & 정제 스트리밍 (Silver)

- **핵심 목표:**  
  - **Sliding Window**와 **Watermark**를 활용해 지연 이벤트를 처리하고, 중복을 제거합니다.

- **삽입 포인트 C — Sliding Window & Watermark**  
  - **목적:** 실시간 지표(1시간 윈도우/5분 슬라이드), 지연 이벤트 허용(예: 10분) 및 정확한 중복 제거  
  - **방법:** withWatermark로 이벤트 시간 기준 허용 지연을 설정 → window('1 hour','5 minutes') 적용

---

## 3과제. 피처 엔지니어링 & 준실시간 라벨링 (Gold/Features)

- **핵심 목표:**  
  - 영상 데이터와 소셜 데이터를 조인하고, **Flajolet–Martin 계열 근사** 및 **CDF/PDF**로 임계치를 도출합니다.

- **삽입 포인트 D — Flajolet–Martin(근사 유니크 카운트) / HyperLogLog++**  
  - **목적:** 거대 스트림에서 고유 사용자 수를 가볍게 추정  
  - **방법:** Spark 내장 approx_count_distinct(col) 함수(HLL++, FM 계열 아이디어)로 대체 가능

- **삽입 포인트 E — 경험적 PDF/CDF 기반 라벨링(상위 p% 구분)**  
  - **목적:** “고성능(상위 10%)” 라벨을 데이터 분포(CDF)로 동적으로 결정  
  - **방법:** (배치/마이크로배치) 히스토그램으로 PDF 추정, 누적합으로 CDF 산출, 상위 p% 컷오프 계산

---

## 4과제. 다수 알고리즘 벤치마킹 & Pareto-Optimal 모델 선정

- **핵심 목표:**  
  - 다목적 최적화(성능, 지연, 해석성 등)에서 **Pareto 전선(Pareto Front)**을 통해 베스트 후보를 자동 선택합니다.

- **삽입 포인트 F — Pareto Front (성능-지연-비용 다목적)**  
  - **목적:** AUC(↑), 추론지연(ms, ↓), 피처수(↓) 등 상충 목표를 동시에 고려  
  - **방법:** 각 후보 모델의 지표 벡터에 대해 지배관계 비교로 Pareto Front 추출

---

## 5과제. 실시간 예측/조회 Streamlit

- **핵심 목표:**  
  - 사용자가 입력한 키워드/영상ID에 대해, 최신 예측 결과와 분포(CDF/PDF) 맥락을 함께 보여줍니다.

- **삽입 포인트 G — CDF/PDF 시각화, Bloom Filter로 쿼리 사전 점검**  
  - **CDF/PDF:** 현재 engagement_24h의 분위(예: 상위 10%) 기준선을 함께 표시  
  - **Bloom Filter:** 입력한 video_id가 “최근 7일 데이터에 존재할 가능성”을 즉시 피드백  
  - **(선택)** “Top-K 상승률” 카드에 Sliding Window(1시간/5분) 집계 결과 반영

---

## 폴더 안내

- **설정 파일:**  
  - `.env.example`, `app.yaml.example`, `logging.yaml`
- **라이브러리:**  
  - `libs/`
  - `session.py` (Spark 세션/설정 로더)  
  - `sampling.py` (Reservoir Sampling)  
  - `metrics.py` (CDF/PDF 컷/라벨)  
  - `pareto.py` (Pareto 전선)  
  - `io.py` (NDJSON/샘플 저장)
- **잡 스크립트:**  
  - `00_fetch_to_landing.py` — 모의 API 페치 + Reservoir 샘플 저장  
  - `10_bronze_batch.py` — Bloom 필터 기반 중복 차단 후 Delta 적재  
  - `20_silver_stream.py` — 파일 소스 스트리밍 + Sliding Window & Watermark 집계  
  - `30_gold_features.py` — HyperLogLog(approx_count_distinct) + CDF 라벨  
  - `40_train_pareto.py` — 다모델 학습 + Pareto-front 산출  
  - `50_predict_stream.py` — (옵션) 간단 예측 append
- **Streamlit 앱:**  
  - `app/app.py` — CDF/PDF 시각화, Bloom 존재 가능성 체크, 테이블 뷰
- **스크립트:**  
  - 초기화/데이터 생성/앱 실행  
    - `reset_all.sh`, `make_stream_data.sh`, `run_streamlit.sh`
- **의존성:**  
  - `requirements.txt` (pyspark, delta-spark, mlflow, streamlit 등)
- **README:**  
  - 퀵스타트와 전체 실행 순서 포함
