from gitlab-registry.cern.ch/linuxsupport/cc7-base
#from centos:latest

ADD data/* data/

# set up enstore
RUN cp data/enstore.repo /etc/yum.repos.d/enstore.repo && echo '**nameserver 8.8.8.8**' >> /etc/resolv.conf
RUN yum update -y && yum install -y enstore tk
RUN source data/setup-enstore && python -m pip install pytest pytest_socket pylint pyfakefs mock coverage  && ln -s  /data/_sqlite3.so /opt/enstore/Python/lib/python2.7/lib-dynload
RUN mkdir -p /home/enstore/site_specific/config
