.PHONY: test coverage

test:
	python -m unittest discover -s doozerlib -p 'distgit_test.py'

coverage:
	coverage run --branch --source doozerlib \
		-m unittest discover -s doozerlib -p 'distgit_test.py' &&\
	coverage report -m
