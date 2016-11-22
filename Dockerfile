FROM postgres:9.6

RUN apt-get -y update
RUN apt-get -y install python-pip postgresql-server-dev-all pgxnclient python-dev
RUN pgxn install multicorn
RUN pip install boto3 botocore
ADD . .
RUN python setup.py install
