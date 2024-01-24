from almalinux:8.9

ADD data/* data/

# set up enstore repo
RUN cp data/enstore.repo /etc/yum.repos.d/enstore.repo
# set up rpm build requirements
RUN dnf update -y && dnf install -y tk which swig Python-enstore-2.7.18 rpm-build swig-enstore make gcc && dnf clean all
RUN dnf  --downloadonly --disablerepo='*' --enablerepo=enstore install -y mtx
RUN rpm -i `find /var/cache/ -name mtx-1.3.12-18fnal.el7.x86_64.rpm` 
RUN dnf -y --enablerepo=powertools install perl-IPC-Run && dnf clean all
#install postgresql stuff
RUN dnf -qy module disable postgresql
RUN dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN dnf install -y postgresql12-libs postgresql12-server postgresql12 postgresql12-devel postgresql12-contrib && dnf clean all

# add git for running `git log` during patchnote generation
RUN yum -y install https://packages.endpointdev.com/rhel/8/main/x86_64/endpoint-repo.noarch.rpm

# set up rpm sign requirements
RUN dnf install -y git emacs-filesystem pcre2 rpm-sign expect
RUN dnf --disablerepo="*" --enablerepo="endpoint" upgrade -y git
RUN cp data/rpmmacros /tmp/rpmmacros && cp data/rpmmacros /etc/rpm/macros && chmod 444 /tmp/rpmmacros && chmod 444 /etc/rpm/macros && cp data/rpm-sign.exp /tmp/rpm-sign.exp && chmod 555 /tmp/rpm-sign.exp && dnf clean all
