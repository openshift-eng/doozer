.PHONY: venv tox lint test install

install:
	./venv/bin/pip install -r requirements-dev.txt -r requirements.txt

venv:
	python3.8 -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements-dev.txt -r requirements.txt
	# source venv/bin/activate

lint:
	./venv/bin/python -m flake8 doozerlib/ tests/

test: lint
	./venv/bin/python -m pytest --verbose --color=yes tests/
