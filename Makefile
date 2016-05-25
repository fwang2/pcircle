PYTHON=`which python`
NAME=`python setup.py --name`
VERSION=`python setup.py --version`
SDIST=dist/$(NAME)-$(VERSION).tar.gz
VENV=$(HOME)/app-pcircle
VDEV=$(HOME)/dev-pcircle
.PHONY:	test check

all: source
dist: source

source:
	$(PYTHON) setup.py sdist

deb:
	$(PYTHON) setup.py --command-packages=stdeb.command bdist_deb

rpm:

	$(PYTHON) setup.py bdist_rpm --require \
		"numpy python-scandir libattr-devel pyxattr python-argparse mpi4py-openmpi python-cffi lru-dict"


install: deploy

uninstall:
	pip uninstall -y pcircle


check:
	find . -name \*.py | grep -v "^test_" | xargs pylint --errors-only --reports=n
	# pep8
	# pyntch
	pyflakes
	# pychecker
	# pymetrics

upload:
	$(PYTHON) setup.py sdist register upload
	$(PYTHON) setup.py bdist_wininst upload

init:
	pip install -r requirements.txt --allow-all-external

update:
	rm ez_setup.py
	wget http://peak.telecommunity.com/dist/ez_setup.py

daily:
	$(PYTHON) setup.py bdist egg_info --tag-date

deploy:
	rm -rf dist
	$(PYTHON) setup.py sdist
	rm -rf $(VENV)
	virtualenv --no-site-packages $(VENV)
	$(VENV)/bin/pip install $(SDIST)
	virtualenv --relocatable $(VENV)

dev-deploy:	
	rm -rf dist
	rm -rf $(VDEV)
	$(PYTHON) setup.py sdist
	virtualenv --no-site-packages $(VDEV)
	$(VDEV)/bin/pip install flake8
	$(VDEV)/bin/pip install -e .

dev-link:	
	rm -rf dist
	$(PYTHON) setup.py sdist
	virtualenv --no-site-packages $(VDEV)
	$(VDEV)/bin/pip install flake8
	$(VDEV)/bin/pip install -e .

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
