FROM python:3.6.4-alpine3.7

RUN apk add --no-cache git make g++ nodejs

WORKDIR /opt/yaldf-server/

# install yaldf-server
COPY . .
RUN make install

EXPOSE 8000

CMD [ "sh", "run.sh" ]
