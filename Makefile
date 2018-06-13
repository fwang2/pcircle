PYTHON=`which python`
NAME=`python setup.py --name`
VERSION=`python setup.py --version`
SDIST=dist/$(NAME)-$(VERSION).tar.gz
VDEV=$(HOME)/pdev
.PHONY:	test check dist

all: dist

dist:
	rm -rf dist
	python setup.py sdist
	python setup.py bdist_wheel

deb:
	$(PYTHON) setup.py --command-packages=stdeb.command bdist_deb

rpm:

	$(PYTHON) setup.py bdist_rpm --require \
		"numpy python-scandir libattr-devel pyxattr python-argparse mpi4py-openmpi python-cffi lru-dict"

install: deploy

uninstall:
	pip uninstall -y pcircle

clean-cache:
	@if [ -d ~/Library/Caches/pip ]; then \
		echo "Found pip cache, cleaned"; \
		rm -rf ~/Library/Cache/pip; \
	fi
	@if [ -d ~/.cache/pip ]; then \
		echo "Found pip cache, cleaned"; \
		rm -rf ~/.cache/pip; \
	fi

check:
	find . -name \*.py | grep -v "^test_" | xargs pylint --errors-only --reports=n
	# pep8
	# pyntch
	pyflakes
	# pychecker
	# pymetrics

daily:
	$(PYTHON) setup.py bdist egg_info --tag-date

deploy:
	rm -rf dist
	$(PYTHON) setup.py sdist
	rm -rf $(VENV)
	virtualenv --no-site-packages $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install --no-cache cffi
	$(VENV)/bin/pip install --no-cache $(SDIST)
	virtualenv --relocatable $(VENV)

deploy-lfs:
	$(PYTHON) setup.py sdist
	virtualenv --no-site-packages $(VENV)
	$(VENV)/bin/pip --no-cache install $(SDIST)
	virtualenv --relocatable $(VENV)

dev:clean-cache
	$(PYTHON) setup.py sdist
	@if [ ! -d $(VDEV) ]; then \
		virtualenv --no-site-packages $(VDEV); \
	fi
	$(VDEV)/bin/pip install -U pip setuptools
	$(VDEV)/bin/pip install argparse
	$(VDEV)/bin/pip install future
	$(VDEV)/bin/pip install flake8
	$(VDEV)/bin/pip install -e .

dev3:clean-cache
	echo "Creating virtual environment at [$(HOME)/pdev3]"
	python3 setup.py sdist
	@if [ ! -d $(HOME)/pdev3 ]; then \
		python3 -m venv $(HOME)/pdev3; \
	fi
	$(HOME)/pdev3/bin/pip3 install -U pip setuptools
	$(HOME)/pdev3/bin/pip3 install future
	$(HOME)/pdev3/bin/pip3 install flake8
	$(HOME)/pdev3/bin/pip3 install -e .


test:
	$(PYTHON) -m unittest discover ./test -v

clean:
	$(PYTHON) setup.py clean
	rm -rf build/ MANIFEST dist build pcircle.egg-info deb_dist
	find . -name '*.pyc' -delete
	find . -name '*.checksums' -delete
	find . -name '*.sig' -delete
	find . -name '*.log' -delete
	find . -name '*pcp_workq*' -delete
	find . -name '.pcircle*' -delete
