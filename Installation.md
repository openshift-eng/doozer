# Dependencies

While Doozer is a pure python application it's heavily dependent on a wide range of internal Red Hat tooling so we've wrapped as much of that as possible into an install script (more on this below). But first you need ensure that you have a few basic requirements setup.



## Kerberos
You must have kerberos authentication setup and be able to kinit as your LDAP user. This is required for interacting with distgit.

## Internal Red Hat Certs
You must have valid corporate IT certificates imported on your system:


```
$ cd /etc/pki/ca-trust/source/anchors
$ sudo curl -O https://password.corp.redhat.com/RH-IT-Root-CA.crt
$ sudo curl -O https://password.corp.redhat.com/legacy.crt
$ sudo curl -k -O https://engineering.redhat.com/Eng-CA.crt
$ sudo update-ca-trust extract
```

## Key Auth to GitHub

You will need to make sure that you can clone from GitHub without a password. If you have not already setup an SSH key in your GitHub account follow [these instructions](https://help.github.com/articles/adding-a-new-ssh-key-to-your-github-account/). This can be tested by running:

`ssh -T git@github.com`

# Run Install Script

With the above completed you can just run the install script via:

`bash <(curl https://raw.githubusercontent.com/openshift/doozer/master/install.sh)`

You **must** run the script as the kerberos user you will be using.

Once complete, checkout the [Usage doc](https://github.com/openshift/doozer/blob/master/Installation.md) for more details.