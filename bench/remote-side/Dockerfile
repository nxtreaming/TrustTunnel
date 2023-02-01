# syntax=docker/dockerfile:1
FROM bench-common

ARG HOSTNAME="server.bench"
ARG PORT=8080
ARG SERVER_BASE_DIR="server_base"
ARG DOWNLOAD_DIR="download"
ARG UPLOAD_DIR="upload"
ARG CONFIG_FILE="nginx.conf"

RUN apt install -y nginx

WORKDIR /bench/$SERVER_BASE_DIR

RUN echo "events {\n\
}\n\
\n\
http {\n\
  sendfile on;\n\
\n\
  server {\n\
    listen        $PORT;\n\
    server_name   $HOSTNAME;\n\
    root          /bench/$SERVER_BASE_DIR/;\n\
\n\
    location /$UPLOAD_DIR {\n\
      autoindex               on;\n\
      client_body_temp_path   /bench/$SERVER_BASE_DIR/$UPLOAD_DIR/;\n\
      create_full_put_path    on;\n\
      client_max_body_size    0;\n\
      dav_methods             PUT;\n\
      dav_access              group:rw  all:r;\n\
    }\n\
\n\
    location /$DOWNLOAD_DIR {\n\
    }\n\
  }\n\
}" >>$CONFIG_FILE

ADD entrypoint.sh /bench/$SERVER_BASE_DIR

ENV CONFIG=$CONFIG_FILE \
    DOWNLOAD_DIR=$DOWNLOAD_DIR \
    UPLOAD_DIR=$UPLOAD_DIR
ENTRYPOINT ./entrypoint.sh "$CONFIG" "$DOWNLOAD_DIR" "$UPLOAD_DIR"
