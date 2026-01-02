.PHONY: install lint format test run-app

install:
\tpython -m pip install -e .[dev]

lint:
\truff check .

format:
\truff format .

test:
\tpytest

run-app:
\tstreamlit run src/asteroid_analysis/app.py
