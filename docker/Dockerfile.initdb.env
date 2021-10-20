FROM python:3.9-slim
WORKDIR /src/app
ADD . .
RUN apt-get update
RUN apt-get install -y git && pip install -r backend/app/requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/src/app"

CMD ["python3", "backend/app/initialize.py"]