# Assignment 04 · Model Training & Pareto Selection

The screenshots in this section illustrate how candidate models are trained on the gold features, logged to MLflow, and filtered via Pareto-front analysis.

## Screenshots
- `screenshots/학습 실행 결과 1.png` & `screenshots/학습 실행 결과 2.png`: Terminal captures of sequential training runs (Logistic Regression, Random Forest, GBT) with timing and metric outputs.
- `screenshots/학습된 모델 저장.png`: File-system confirmation that serialized models/artifacts were persisted to the designated `project/artifacts/` directory.
- `screenshots/모델 성능 비교.png`: Comparative table or plot showing AUC-PR, latency, and feature counts used to evaluate each model.
- `screenshots/MLflow 실험 추적.png`: MLflow UI snippet evidencing experiment tracking for the training session.
- `screenshots/Pareto Front 아티팩트.png`: Visualization or JSON view of the Pareto-front artifact highlighting non-dominated models retained for deployment.
