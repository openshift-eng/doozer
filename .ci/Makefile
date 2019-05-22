.PHONY: test coverage

test:
	python -m unittest discover -s tests -p '*_test.py'

coverage:
	coverage run --branch --source doozerlib \
		-m unittest discover -s tests -p '*_test.py' &&\
	coverage report -m
