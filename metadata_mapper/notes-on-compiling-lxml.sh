# Using the AWS CLI: launch an EC2 instance of the Amazon Linux 2 AMI.
# See https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html for runtimes.

# I'm targeting the following environment:
# Amazong Linux 2
# Kernel 4.14
# x86_64 architecture
# Python 3.9
# US East 1 region

SECURITY_GROUP_IDS=${SECURITY_GROUP_IDS:-"sg-0d1e4da1356df290e"}
SUBNET_ID=${SUBNET_ID:-"subnet-0b6b5c7bb67a2ba3d"}
AMI_ID=${AMI_ID:-"ami-065efef2c739d613b"}

# From the guest machine:
# aws ec2 run-instances
#     --image-id $AMI_ID \
#     --instance-type t2.medium \
#     --key-name lxml-install \
#     --security-group-ids $SECURITY_GROUP_IDS \
#     --subnet-id $SUBNET_ID  \
#     --associate-public-ip-address

#
# GUEST MACHINE
#

# Install AWS dev bundle
sudo yum update -y
sudo yum groupinstall -y "Development Tools"

# Install dependencies useful for compiling Python 3.
sudo yum install -y openssl-devel zlib-devel bzip2-devel readline-devel sqlite-devel

# Install Python 3.9 from source (see https://www.python.org/downloads/).
wget https://www.python.org/ftp/python/3.9.0/Python-3.9.0.tgz
tar -xzf Python-3.9.0.tgz
cd Python-3.9.0
./configure --with-optimizations --with-ensurepip=install
make -j4
cd -

# Create a virtual Python environment with the new version of Python just installed.
mkdir lxml-build
cd lxml-build
../Python-3.9.0/python -m venv 🐍
. 🐍/bin/activate

# Install lxml.
pip install lxml

# Quick test:
python -c "from lxml import etree"

# Bundle it up for Lambda.
cd 🐍/lib/python3.9/site-packages/lxml*
zip -r ~/lxml-package-python3-9-amazon-linux-2.zip lxml*
cd ~

# From the guest machine, download with something like:
# scp -i lxml-install.pem ec2-user@54.145.251.19:lxml-package-python3-9-amazon-linux-2.zip .
