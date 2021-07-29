# stdlib
from github import Github
import datetime
import yaml
import time
import os
from doozerlib import constants

# external
import click

# doozerlib
from doozerlib.cli import cli, pass_runtime

github_token=os.getenv(constants.GIT_TOKEN)
github_client=Github(github_token)
repo = github_client.get_repo("openshift/ocp-build-data")

@cli.command("images:upstreampulls", short_help="Check upstream reconciliation PRs in open status")
@pass_runtime
def images_upstreampulls(runtime):
    runtime.initialize(clone_distgits=False, clone_source=False)
    group=runtime.group
    version=group.split("-")[-1]
    retdata={}
    contents=repo.get_contents("images",ref="openshift-"+version)
    for content_file in contents:
        ydata=yaml.load(content_file.decoded_content,Loader=yaml.FullLoader)
        if 'content' not in ydata:
            continue
        if 'git' not in ydata['content']['source']:
            continue
        upstream_project=ydata['content']['source']['git']['url'].split(':')[-1].split('/')[0]
        upstream=ydata['content']['source']['git']['url'].split('/')[-1].split('.')[0]
        if upstream_project == "openshift-priv":
            xrepo=github_client.get_repo("openshift/"+upstream)
        else:
            xrepo=github_client.get_repo(upstream_project+"/"+upstream)
        pulls=xrepo.get_pulls(state='open',sort='created')
        for pr in pulls:
            if pr.user.login == "openshift-bot" and pr.base.ref == "release-"+version:
                if pr.assignee is not None:
                    if pr.assignee.email is not None:
                        if pr.assignee.email not in retdata.keys():
                            retdata[pr.assignee.email]={}
                        if upstream not in retdata[pr.assignee.email].keys():
                            retdata[pr.assignee.email][upstream]=[]
                        retdata[pr.assignee.email][upstream].append("[{} ][created_at:{}]".format(pr.html_url,pr.created_at))
                    else:
                        if "no assignee" not in retdata.keys():
                            retdata["no assignee"]={}
                        if upstream not in retdata["no assignee"].keys():
                            retdata["no assignee"][upstream]=[]
                        retdata["no assignee"][upstream].append("[{} ][created_at:{}]".format(pr.html_url,pr.created_at))
    # print dict data
    for key,val in retdata.items():
        if len(val)==0:
            continue
        print(">[{}]".format(key))
        for img,prs in val.items():
            print(" -[{}]".format(img))
            for pr in prs:
                print("   {}".format(pr))

