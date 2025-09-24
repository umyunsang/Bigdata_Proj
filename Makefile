PY=python

.PHONY: venv install scaffold fetch bronze silver gold train predict ui package fetch_cli bronze_cli silver_cli gold_cli train_cli ui_cli

venv:
	$(PY) -m venv .venv

install:
	. .venv/bin/activate && pip install -r requirements.txt

scaffold:
	$(PY) scripts/scaffold_project.py

fetch:
	$(PY) video-social-rtp-snippets/jobs/00_fetch_to_landing.py

bronze:
	$(PY) video-social-rtp-snippets/jobs/10_bronze_batch.py

silver:
	$(PY) video-social-rtp-snippets/jobs/20_silver_stream.py

gold:
	$(PY) video-social-rtp-snippets/jobs/30_gold_features.py

train:
	$(PY) video-social-rtp-snippets/jobs/40_train_pareto.py

predict:
	$(PY) video-social-rtp-snippets/jobs/50_predict_stream.py

ui:
	$(PY) video-social-rtp-snippets/app/app.py

package:
	zip -r submission/video-social-rtp-snippets.zip video-social-rtp-snippets -x "*.venv*" "*.git*" "*__pycache__*"
	zip -r submission/Bigdata_Proj_submission.zip submission -x "*.git*"

# CLI-based (project package)
fetch_cli:
	$(PY) -m video_social_rtp.cli fetch

bronze_cli:
	$(PY) -m video_social_rtp.cli bronze

silver_cli:
	$(PY) -m video_social_rtp.cli silver

gold_cli:
	$(PY) -m video_social_rtp.cli gold --top-pct 0.9

train_cli:
	$(PY) -m video_social_rtp.cli train

ui_cli:
	$(PY) -m video_social_rtp.cli ui
