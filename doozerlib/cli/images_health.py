# stdlib
import collections
import datetime
import yaml
import time

# external
import click

# doozerlib
from doozerlib.cli import cli, pass_runtime


BuildInfo = collections.namedtuple('BuildInfo', 'record_name, task_id task_state ts build_url, task_url, dt')

millis_hour = 1000 * 60 * 60
millis_day = millis_hour * 24
now_unix_ts = int(round(time.time() * 1000))  # millis since the epoch


@cli.command("images:health", short_help="Create a health report for this image group (requires DB read)")
@click.option('--limit', default=100, help='How far back in the database to search for builds')
@click.option('--url-markup', default='slack', help='How to markup hyperlinks (slack, github)')
@pass_runtime
def images_health(runtime, limit, url_markup):
    runtime.initialize(clone_distgits=False, clone_source=False)

    concerns = dict()
    for image_meta in runtime.image_metas():

        image_concerns = get_concerns(image_meta.qualified_key, runtime, limit, url_markup)
        if image_concerns:
            concerns[image_meta.qualified_key] = image_concerns

    # We should now have a dict of qualified_key => [concern, ...]
    if not concerns:
        runtime.logger.info('No concerns to report!')
        return

    print(yaml.dump(concerns, default_flow_style=False, width=10000))


def get_concerns(image, runtime, limit, url_markup):
    image_concerns = []

    def add_concern(msg):
        image_concerns.append(msg)

    def url_text(url, text):
        if url_markup == 'slack':
            return f'<{url}|{text}>'
        if url_markup == 'github':
            return f'[{text}]({url})'
        raise IOError(f'Unknown markup mode: {url_markup}')

    records = query(image, runtime, limit)

    if not records:
        add_concern('Image build has never been attempted')
        return image_concerns

    latest_success_idx = -1
    latest_success_bi = None
    latest_success_bi_task_url = ''
    latest_success_bi_build_url = ''
    latest_success_bi_dt = ''

    for idx, record in enumerate(records):
        # record=( jobid, state, timestamp, joburl)
        if record[1] == 'success':
            latest_success_idx = idx
            latest_success_bi = record
            latest_success_bi_task_url = f"https://brewweb.engineering.redhat.com/brew/taskinfo?taskID={latest_success_bi[0]}"
            latest_success_bi_build_url = latest_success_bi[3]
            latest_success_bi_dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(latest_success_bi[2] / 1000))
            break

    latest_attempt_build_url = records[0][3]
    latest_attempt_task_url = f"https://brewweb.engineering.redhat.com/brew/taskinfo?taskID={records[0][0]}"
    oldest_attempt_bi_dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(records[-1][2] / 1000))

    if latest_success_idx != 0:
        msg = f'Latest attempt {url_text(latest_attempt_task_url, "failed")} ({url_text(latest_attempt_build_url, "jenkins job")}); '
        # The latest attempt was a failure
        if latest_success_idx == -1:
            # No success record was found
            msg += f'Failing for at least the last {len(records)} attempts / {oldest_attempt_bi_dt}'
        else:
            msg += f'Last {url_text(latest_success_bi_task_url, "success")} was {latest_success_idx} attempts ago on {latest_success_bi_dt}'

        add_concern(msg)

    else:
        if older_than_two_weeks(latest_success_bi):
            # This could be made smarter by recording rebase attempts in the database..
            add_concern(f'Last {url_text(latest_success_bi_task_url, "build")} ({url_text(latest_success_bi_build_url, "jenkins job")}) was over two weeks ago.')

    return image_concerns


def query(name, runtime, limit=100):
    domain = "`log_build`"
    fields_str = "`brew_task_id`, `brew_task_state`, `time_unix`, `jenkins_build_url`"
    where_str = f'WHERE `group`="{runtime.group_config.name}" and `dg_qualified_key`="{name}" and `time_unix` is not null'
    sort_by_str = ' ORDER BY `time_unix` DESC'

    expr = f'SELECT {fields_str} FROM {domain} {where_str} {sort_by_str}'
    return runtime.db.select(expr, limit=int(limit))


def extract_buildinfo(record):
    """
    Returns a tuple with record information, (name, task_id, task_state, unix_ts, build_url)
    """
    # Each record looks something like:
    # {'Attributes': [{'Name': 'brew.task_state', 'Value': 'failure'},
    #                 {'Name': 'build.time.unix', 'Value': '1599799663698'},
    #                 {. ......... },],
    #   'Name': '20200911.043009.37.e192b58b7e590d4a5156777527bdab72'}
    name = record['Name']
    attr_list = record['Attributes']
    attrs = dict()
    for attr in attr_list:
        attrs[attr['Name']] = attr['Value']

    return BuildInfo(
        record_name=name,
        task_id=attrs['brew.task_id'],
        ts=int(attrs['build.time.unix']),
        dt=datetime.datetime.fromtimestamp(int(attrs['build.time.unix']) / 1000.0),
        task_state=attrs['brew.task_state'],
        build_url=attrs['jenkins.build_url'],
        task_url=f"https://brewweb.engineering.redhat.com/brew/taskinfo?taskID={attrs['brew.task_id']}"
    )


def older_than_two_weeks(task_record):
    return task_record[2] - now_unix_ts > 2 * 7 * millis_day
