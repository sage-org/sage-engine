# SaGe server
Python implementation of SaGe, a preemptive query engine for Basic Graph pattern evaluation.

# Installation

Installation in a [virtualenv](https://virtualenv.pypa.io/en/stable/) is **strongly advised!**

Requirements:
* [pip](https://pip.pypa.io/en/stable/)
* [npm](https://nodejs.org/en/) (shipped with Node.js on most systems)
* **gcc/clang** with **c++11 support**
* **Python Development headers**
> You should have the `Python.h` header available on your system.   
> For example, for Python 3.6, install the `python3.6-dev` package on Debian/Ubuntu systems.

```
git clone https://github.com/Callidon/sage-bgp
cd sage-engine/
make install
```

# Launch server

The configuration file for the SaGe experimental setup is `data/watdiv_config.yaml`.

```
# launch server with 1 worker on port 8000 using Gunicorn
./run.sh data/watdiv_config.yaml 1 8000
```

# Run with Docker

```
docker build -t sage .
docker run -p 8000:8000 sage:latest sh run.sh data/watdiv_config.yaml
```

# Documentation

To generate the documentation, you must install the following dependencies
```
pip install sphinx sphinx_rtd_theme sphinxcontrib-httpdomain
```

Then, navigate in the `docs` directory and generate the documentation
```
cd docs/
make html
open build/html/index.html
```
