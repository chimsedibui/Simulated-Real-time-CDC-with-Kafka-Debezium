FROM python:3.11-slim
WORKDIR /validation
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
CMD ["python", "check_data.py"]
