from almalinux:8.9
# Docker image that builds Python-enstore and friends for almalinux 8
# to build: run command /data/build_enstore_python_rpm.sh
# resulting rpms end up in in /root/rpmbuild/RPMS/


RUN dnf -y install rpmdevtools && rpmdev-setuptree
RUN dnf install -y  python2 python2-devel  python2-libs python2-pip-wheel  which wget swig tk-devel tcl-devel expat-devel sqlite-devel bzip2-devel openssl-devel ncurses-devel readline-devel gcc make
RUN dnf -y install https://packages.endpointdev.com/rhel/8/main/x86_64/endpoint-repo.noarch.rpm
RUN dnf update -y && dnf install -y git emacs-filesystem pcre2 procps-ng
RUN dnf --disablerepo="*" --enablerepo="endpoint" upgrade -y git





# set up rpm sign requirements
RUN dnf install -y rpm-sign expect
ADD data/* data/
RUN cp data/rpmmacros /tmp/rpmmacros
RUN cp data/rpmmacros /etc/rpm/macros
RUN chmod 444 /tmp/rpmmacros
RUN chmod 444 /etc/rpm/macros
RUN cp data/rpm-sign.exp /tmp/rpm-sign.exp
RUN chmod 555 /tmp/rpm-sign.exp
