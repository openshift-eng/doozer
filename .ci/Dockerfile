FROM centos:7

RUN yum install -y epel-release make gcc krb5-devel python-devel
RUN yum install -y python-pip
RUN pip install koji

WORKDIR /code
COPY requirements* ./
RUN pip install -r requirements-test.txt

ENV PYTHONPATH /code/doozerlib

ENTRYPOINT ["make", "-f", ".ci/Makefile"]
CMD ["test"]
