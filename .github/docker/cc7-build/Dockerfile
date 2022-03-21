from gitlab-registry.cern.ch/linuxsupport/cc7-base
# from centos:latest

ADD data/* data/

# set up enstore repo
RUN cp data/enstore.repo /etc/yum.repos.d/enstore.repo
RUN yum update -y && yum install -y tk

# set up rpm build requirements
RUN yum install -y rpm-build Python-enstore2.7-16* swig-enstore make gcc
RUN yum install -y postgresql11-libs postgresql11-server postgresql11 postgresql11-devel postgresql11-contrib
# don't ask me why getting libmtx.so is so hard
RUN yum --downloadonly --disablerepo='*' --enablerepo=enstore install -y mtx
RUN rpm -i /var/cache/yum/x86_64/7/enstore/packages/mtx-1.3.12-18fnal.el7.x86_64.rpm 

# add git for running `git log` during patchnote generation
RUN yum -y install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm
RUN yum update -y && yum install -y git emacs-filesystem pcre2
RUN yum --disablerepo="*" --enablerepo="endpoint" upgrade -y git
