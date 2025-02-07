check: black lint

black:
	black .

lint:
	pylint . --max-line-length=120