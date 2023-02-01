# syntax=docker/dockerfile:1
FROM bench-common

ARG UPLOAD_FILENAME="upload.dat"

RUN pip install pipreqs

COPY ./local_side.py /bench/

WORKDIR /bench/
RUN pipreqs . && \
    pip install -r requirements.txt && \
    rm requirements.txt && \
    dd bs=$((1024 * 1024)) count=1024 if=/dev/random of="$UPLOAD_FILENAME"

ENV UPLOAD_FILENAME=$UPLOAD_FILENAME
