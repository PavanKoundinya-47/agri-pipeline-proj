FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the code
COPY agri-pipeline/pipeline pipeline/
COPY agri-pipeline/run_pipeline.py .

# Create dirs
RUN mkdir -p logs reports data/processed data/raw

CMD ["python", "run_pipeline.py", \
     "--raw_dir", "data/raw", \
     "--processed_dir", "data/processed", \
     "--report_path", "reports/data_quality_report.csv"]