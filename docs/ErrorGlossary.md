##  DistGit does not exist for given name

In the situation where the given name for a distgit does not represent an actual distgit repo, the error from rhpkg is non-sensical:

```
Please ensure you are in devel LDAP group. Run `ssh shell.devel.redhat.com groups $USER` to check.
<<
stderr>>Cloning into '~/workspace/distgits/containers/ose-cluster-network-operator'...
R access for containers/ose-cluster-network-operator DENIED to ocp-build
(Or there may be no repository at the given path. Did you spell it correctly?)
fatal: Could not read from remote repository.
 
Please make sure you have the correct access rights
and the repository exists.
Could not execute clone: Failed to execute command.
```

The key to this error is `Or there may be no repository at the given path. Did you spell it correctly?`.
Seeing this error is almost never due to actual permissions/LDAP issues. It just means that either:

- The dist-git name is correct as requested from RCM but does not yet exist. If there is a ticket for the distgit creation find it and ping RCM to get things moving. If not, the dev never requested it from the Comet form.
- The dist-git you want exists but the name is spelled wrong, meaning that the name of you config yml file is incorrect. Verify what the correct name should be and update the name of your file.

## Missing Product Listings

`BuildError: package (container) [IMAGE_NAME] not in list for tag [TAG]`

This simply means that RCM did not fully complete the new image creation process and never added the image to the product listings. These are essentially a database in the RCM systems that link up distgits with known products that are allowed to be released. Without that entry OSBS will refuse to build the image.

Resolution: Find the original RCM Jira ticket for the creation of this image, re-open and request: 
`[IMAGE_NAME] was not added to the product listings for [TAG], please add the image to the listings so it can be built.`
