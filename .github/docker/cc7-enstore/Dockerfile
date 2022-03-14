from gitlab-registry.cern.ch/linuxsupport/cc7-base
# from centos:latest

ADD data/* data/

# set up enstore
RUN cp data/enstore.repo /etc/yum.repos.d/enstore.repo
RUN yum update -y && yum install -y enstore tk
RUN source data/setup-enstore && python -m pip install pytest pylint pyfakefs mock
# add git for running `git log` during patchnote generation
RUN yum -y install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm
RUN yum update -y && yum install -y git
