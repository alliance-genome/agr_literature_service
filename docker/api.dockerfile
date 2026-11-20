ARG REG
ARG ALLIANCE_RELEASE

# Records which commit the build context is checked out at (SCRUM-6878), however the build
# is started: the stage and prod redeploys run docker compose directly, not the Makefile.
# Only the two small files reach the final image; .git does not.
FROM python:3.11-alpine3.20 AS build_info
RUN apk add --no-cache git
COPY .git /tmp/repo.git
RUN mkdir /build_info && \
    (git -c safe.directory='*' --git-dir=/tmp/repo.git rev-parse --short=8 HEAD || echo unknown) \
        > /build_info/commit && \
    date -u +%Y-%m-%dT%H:%M:%SZ > /build_info/build_date

FROM python:3.11-alpine3.20

RUN apk update && apk add --no-cache git gcc musl-dev postgresql-dev libffi-dev

WORKDIR /usr/local/bin/src/literature
RUN mkdir -p /usr/local/bin/src/literature/agr_literature_service
ADD ./agr_literature_service /usr/local/bin/src/literature/agr_literature_service
COPY ./requirements.txt .
COPY ./logging.conf .
COPY ./gunicorn.conf.py .
COPY ./uvicorn_worker.py .
COPY ./gunicorn_logging.py .
RUN pip3 install --upgrade pip
RUN pip3 install wheel
RUN pip3 install "Cython<3.0" pyyaml --no-build-isolation

RUN pip3 install -r requirements.txt

# Which commit this image was built from (SCRUM-6878). The build_info stage reads it from .git;
# GIT_SHA/BUILD_DATE passed in by the Makefile take precedence (and mark local changes -dirty).
COPY --from=build_info /build_info /etc/abc_build_info
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown
ENV GIT_SHA=${GIT_SHA} BUILD_DATE=${BUILD_DATE}
LABEL org.opencontainers.image.revision=${GIT_SHA} org.opencontainers.image.created=${BUILD_DATE}

# Use gunicorn with uvicorn workers for multi-process handling
CMD ["gunicorn", "agr_literature_service.api.main:app", "-c", "gunicorn.conf.py"]