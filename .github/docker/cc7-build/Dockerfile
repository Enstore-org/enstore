from gitlab-registry.cern.ch/linuxsupport/cc7-base
# from centos:latest

ADD data/* data/

# set up enstore repo
RUN cp data/enstore.repo /etc/yum.repos.d/enstore.repo
RUN yum update -y && yum install -y tk

# set up rpm build requirements
RUN yum install -y rpm-build Python-enstore2.7-16* swig-enstore make gcc
RUN yum install -y postgresql11-libs postgresql11-server postgresql11 postgresql11-devel postgresql11-contrib
