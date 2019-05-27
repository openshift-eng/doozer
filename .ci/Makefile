.PHONY: test coverage

test:
	python -m unittest discover

coverage:
	coverage run --branch --source doozerlib -m unittest discover &&\
	coverage report -m
