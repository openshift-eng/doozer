import sys
import click
import yaml
from doozerlib.cli import cli
from doozerlib.comment_on_pr import CommentOnPr
from doozerlib.logutil import getLogger

logger = getLogger(__name__)


@cli.command("comment-on-pr:from-job",
             short_help="Lists the PRs that need to be commented from the job id.")
@click.option("-j", "--job", required=True, metavar='JOB',
              help="Name of the job. Eg: ocp4/123")
def comment_from_job(job: str):
    """
    Comment on PRs of images built from the job.

    Steps:
    - Gets the list of images built from the job, from art-dashboard-server API endpoint.
    - Finds the PR number from the merge commit.
    - Checks to see if the PR has already been commented by the bot. If it hasn't, send back the list of PRs to update.
    """
    job_name, job_id = job.split("/")
    if job_name != "ocp4":
        logger.error("Can only process OCP4 jobs for now")
        sys.exit(1)

    result = CommentOnPr(job_id=job_id).run()
    print(yaml.dump(result))
