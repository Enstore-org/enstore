apt update -y
apt install -y yum-utils
cp data/enstore.repo /etc/yum.repos.d/enstore.repo
yum update -y
yum install -y enstore
cp data/setup-enstore /tmp/setup-enstore
