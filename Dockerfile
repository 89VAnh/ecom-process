FROM amazon/aws-glue-libs:5
USER root

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt