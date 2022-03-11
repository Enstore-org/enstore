from gitlab-registry.cern.ch/linuxsupport/cc7-base
# from centos:latest

ADD data/* data/

# set up enstore
RUN cp data/enstore.repo /etc/yum.repos.d/enstore.repo
RUN yum update -y && yum install -y enstore tk
RUN source data/setup-enstore && python -m pip install pytest pylint pyfakefs mock pytest-cov
