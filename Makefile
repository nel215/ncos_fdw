test:
	PYTHONPATH=. py.test --pep8 --capture=no .
psql-build:
	docker build -t multicorn .
psql-run: psql-build
	docker run -it -p "5432:5432" --net=host multicorn:latest
s3-run:
	docker run -it -p "4569:4569" --net=host lphoward/fake-s3:latest
