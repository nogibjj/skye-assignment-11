install:
	python3 -m venv venv
	venv/bin/pip3 install --upgrade pip &&\
		venv/bin/pip3  install -r requirements.txt

format:
	venv/bin/black mylib/*.py

lint:
	venv/bin/ruff check mylib/*.py

container-lint:
	venv/bin/docker run --rm -i hadolint/hadolint < Dockerfile

run :
	venv/bin/python3 main.py
		
all: install lint format run