#!/usr/bin/env bash

RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m'

show_cmd(){
	# print and run command
	set -x
	eval "$@"
	if [ $? -ne 0 ]; then
		printf "${RED} Failed running critical command! :( \n${NC}"
		exit 1
	fi
	set +x
}

if [[ $EUID -eq 0 ]]; then
   printf "${RED}This script must NOT be run as root.\n${NC}"
   exit 1
fi

printf "
${GREEN}
        Welcome to the Doozer Installation
***************************************************
${YELLOW}
Before continuing, please be sure that you have
kerberos authentication setup and have the internal
Red Hat CA certificates installed. Follow the
instructions here:
https://github.com/openshift-eng/doozer/blob/master/Installation.md

NOTE: Some commands will be run with sudo.
Please enter sudo password when prompted.
${NC}
"

read -n 1 -s -r -p "Press any key to continue"

echo ""

KERB_ID=$(whoami)
while true; do
	printf "${YELLOW}This must be run with your kerberos user.\nIs ${KERB_ID} the correct user?\n${NC}"
	read -p "Please answer (Y/n) " yn
	case $yn in
		[Yy]* ) break;;
		[Nn]* ) exit;;
		* ) echo "Please answer yes or no.";;
	esac
done

PY_BIN=$(which python)
if [ $? -ne 0 ]; then
    printf "${RED} No python command available! Please install python 2.\n${NC}"
    exit 1
fi

PY_VER=$(python -c 'import sys; print(sys.version_info[0])')
if [ "$PY_VER" -ne "2" ]
then
	printf "${RED} You must install doozer in a Python 2 context.\n Python 3 is unsupported!\n${NC}"
	exit 1
fi

ARCH=$(uname -i)

printf "\nRecommend installing doozer to a virtual environment.\n"
printf "Current Python: ${GREEN}${PY_BIN}${NC}\n"
while true; do
	read -p "Is this the correct desired python? (Y/n) " yn
	case $yn in
		[Yy]* ) break;;
		[Nn]* ) exit;;
		* ) echo "Please answer yes or no.";;
	esac
done

# check if in virtualenv for later
if [[ "$VIRTUAL_ENV" == "" ]]; then
    USER_OPT="--user"
else
    USER_OPT=""
fi


printf "${GREEN}Installing required yum/dnf repos...${NC}\n"

show_cmd sudo dnf config-manager --add-repo https://gitlab.cee.redhat.com/platform-eng-core-services/internal-repos/raw/master/rhel/rhel-7.repo
show_cmd sudo dnf config-manager --add-repo http://download.devel.redhat.com/rel-eng/RCMTOOLS/rcm-tools-rhel-7-server.repo

# only needed for RHEL?
# show_cmd wget https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
# show_cmd dnf install -y epel-release-latest-7.noarch.rpm

printf "${GREEN}Installing imagebuilder...${NC}\n"

show_cmd wget -O imagebuilder.rpm "http://download.eng.bos.redhat.com/brewroot/vol/rhel-7/packages/imagebuilder/0.0.5/1.el7eng/${ARCH}/imagebuilder-0.0.5-1.el7eng.${ARCH}.rpm"
show_cmd sudo dnf install -y imagebuilder.rpm

printf "${GREEN}Installing misc dependencies...${NC}\n"

show_cmd sudo dnf install -y git tito koji rhpkg krb5-devel python3-devel python3-pip python3-rpm gcc

printf "${GREEN}Upgrading pip...${NC}\n"

show_cmd pip install ${USER_OPT} --upgrade pip

printf "${GREEN}Checking docker install...${NC}\n"

which docker
if [ $? -ne 0 ]; then
	printf "${GREEN}Installing docker...${NC}\n"
	show_cmd sudo dnf install -y docker

	grep -q docker /etc/group
	if [ $? -ne 0 ]; then
		show_cmd sudo groupadd docker
		show_cmd sudo usermod -aG docker ${KERB_ID}
	fi
fi

# install toml for below mod script
# installing as sudo because need to edit as sudo
show_cmd sudo pip install toml

printf "${GREEN}Updating /etc/containers/registries.conf...${NC}\n"
sudo python - <<- EOF
	import toml
	brew = 'brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888'
	f = open('/etc/containers/registries.conf', 'r+')
	r = toml.load(f)
	r['registries']['search']['registries'].insert(0, brew)
	r['registries']['insecure']['registries'].insert(0, brew)
	f.seek(0)
	toml.dump(r, f)
	f.truncate()
	f.close()
EOF

printf "${GREEN}Starting install of doozer...${NC}\n"


python -c "$(curl -fsSL https://raw.githubusercontent.com/junaruga/rpm-py-installer/master/install.py)"
pip install ${USER_OPT} -r <(curl https://raw.githubusercontent.com/openshift-eng/doozer/master/requirements.txt)

show_cmd pip install ${USER_OPT} rh-doozer

printf "${GREEN}Doozer install complete!${NC}\n"

printf "${YELLOW}Now we will restart docker and confirm things are working.${NC}\n"

show_cmd sudo systemctl enable docker
show_cmd sudo systemctl restart docker

show_cmd sudo docker pull brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/rhel7:7-released

printf "${GREEN} Complete! \o/ - You may now use doozer.\n"
printf "However, you may need to logout and back in for docker to work.\n"
printf "https://github.com/openshift-eng/doozer/blob/master/Usage.md \n${NC}"