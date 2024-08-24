FROM python:3.10
WORKDIR /app
ADD . .
RUN pip install -r requirements.txt
ENV PYTHONPATH=/app
CMD ["python", "main.py"]