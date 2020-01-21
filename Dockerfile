FROM python:3.7-alpine

RUN apk add --no-cache git make gcc g++ bash postgresql-dev python3-dev musl-dev curl

# for cryptography, need to install libffi-dev
RUN apk add --no-cache libffi-dev

# install poetry
RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python

WORKDIR /opt/sage-engine/

RUN pip install pybind11==2.2.4
RUN source ~/.poetry/env

COPY poetry.lock pyproject.toml ./
# install grpcio first to be faster than a simple poetry install
# super hack-ish from: https://github.com/grpc/grpc/issues/18150
# from 20 minutes to almost less than 1.. thank you :D
# tempory fix until someone release a python3.7-alpine grpcio wheel
RUN echo 'manylinux1_compatible = True' > /usr/local/lib/python3.7/site-packages/_manylinux.py
RUN pip install grpcio
# roll back
RUN rm /usr/local/lib/python3.7/site-packages/_manylinux.py
# generate the requirements.txt from poetry and then use pip to install
RUN ~/.poetry/bin/poetry export -f requirements.txt -v > requirements.txt
# install using poetry
RUN pip install -r requirements.txt

COPY . /opt/sage-engine

# now re run poetry for installing but without using the creation of virtualenv. no need we are in a container ><
# thus no need to install dev dependencies it's a production container
RUN ~/.poetry/bin/poetry config virtualenvs.create false && ~/.poetry/bin/poetry install --no-dev --extras "hdt postgres"

CMD [ "sage" ]
