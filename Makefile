.PHONY: install install-py install-web

install: install-py install-web

install-py:
	pip install pybind11
	pip install -r requirements.txt

install-web:
	cd http_server/static && npm install --production
