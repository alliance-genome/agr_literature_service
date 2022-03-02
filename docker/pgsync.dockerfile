FROM python:3.9-slim
WORKDIR /src/app
RUN apt-get update
RUN apt-get install -y libpq-dev gcc && pip install psycopg2-binary && pip install pgsync
COPY backend/app/initialize.py initialize.py
COPY pgsync_start.sh pgsync_start.sh

CMD ["sh", "pgsync_start.sh"]