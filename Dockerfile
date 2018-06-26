FROM python:3.6.5-alpine3.7

RUN apk add --no-cache git make g++ nodejs

WORKDIR /opt/sage-engine/

COPY . .
RUN make install

EXPOSE 8000

CMD [ "python", "sage.py" ]
