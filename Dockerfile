FROM python:3.9.18-bullseye
WORKDIR /app
ADD . .
RUN pip install -r requirements.txt
ENV PYTHONPATH=/app
CMD ["python", "main.py"]