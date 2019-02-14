FROM python:3.6.5-alpine3.7

RUN apk add --no-cache git make g++ nodejs

WORKDIR /opt/sage-engine/

COPY . .
RUN pip install pybind11==2.2.4
RUN pip install -r requirements.txt
RUN pip install -e .[hdt]

EXPOSE 8000

CMD [ "sage" ]
