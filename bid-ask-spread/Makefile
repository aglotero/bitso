build:
	python setup.py sdist bdist_wheel

test:
	python -m pytest tests

lint:
	black .
	flake8 .

run:
	python -m bidaskspread

clean:
	rm -rf __pycache__
	rm -rf build
	rm -rf dist
	rm -rf bidaskspread.egg-info
	rm -rf tmp_*/
	rm -rf bid_ask_pread_output/
	rm -rf tests/__pycache__
	rm -rf bidaskspread/__pycache__
