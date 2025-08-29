FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agri-pipeline/pipeline pipeline/
COPY agri-pipeline/run_pipeline.py .
COPY agri-pipeline/tests tests/

RUN mkdir -p logs reports data/processed data/raw && chmod -R 777 logs reports data


CMD ["python", "run_pipeline.py", \
     "--raw_dir", "data/raw", \
     "--processed_dir", "data/processed", \
     "--report_path", "reports/data_quality_report.csv"]