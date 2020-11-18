# stdlib
import collections
import datetime
import yaml
import time

# external
import click

# doozerlib
from doozerlib.cli import cli, pass_runtime


@cli.command("images:health", short_help="Create a health report for this image group (requires DB read)")
@click.option('--limit', default=100, help='How far back in the database to search for builds')
@click.option('--url-markup', default='slack', help='How to markup hyperlinks (slack, github)')
@pass_runtime
def images_health(runtime, limit, url_markup):
    runtime.initialize(clone_distgits=False, clone_source=False)
    domain = f'`ART_{runtime.datastore}_build`'
    sort_by_str = ' ORDER BY `build.time.unix` DESC'

    BuildInfo = collections.namedtuple('BuildInfo', 'record_name, task_id task_state ts build_url, task_url, dt')
    fields_str = "`brew.task_id`, `brew.task_state`, `build.time.unix`, `jenkins.build_url`"
    now_unix_ts = int(round(time.time() * 1000))  # millis since the epoch
    millis_hour = 1000 * 60 * 60
    millis_day = millis_hour * 24

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

    def url_text(url, text):
        if url_markup == 'slack':
            return f'<{url}|{text}>'
        if url_markup == 'github':
            return f'[{text}]({url})'
        raise IOError(f'Unknown markup mode: {url_markup}')

    concerns = dict()
    for image_meta in runtime.image_metas():
        image_concerns = []  # As list of concerns about this image
        concerns[image_meta.qualified_key] = image_concerns
        where_str = f'WHERE `group`="{runtime.group_config.name}" and `dg.qualified_key`="{image_meta.qualified_key}" and `build.time.unix` is not null'
        expr = f'SELECT {fields_str} FROM {domain} {where_str}  {sort_by_str}'
        records = runtime.db.select(expr, limit=int(limit))

        def add_concern(msg):
            image_concerns.append(msg)

        if not records:
            add_concern('Image build has never been attempted')
            continue  # Nothing

        latest_success_idx = -1
        latest_success_bi = None
        for idx, record in enumerate(records):
            bi = extract_buildinfo(record)
            if bi.task_state == 'success':
                latest_success_idx = idx
                latest_success_bi = bi
                break

        latest_attempt_bi = extract_buildinfo(records[0])
        oldest_attempt_bi = extract_buildinfo(records[-1])

        if latest_success_idx != 0:
            msg = f'Latest attempt {url_text(latest_attempt_bi.task_url, "failed")} ({url_text(latest_attempt_bi.build_url, "jenkins job")}); '
            # The latest attempt was a failure
            if latest_success_idx == -1:
                # No success record was found
                msg += f'Failing for at least the last {len(records)} attempts / {oldest_attempt_bi.dt}'
            else:
                msg += f'Last {url_text(latest_success_bi.task_url, "success")} was {latest_success_idx} attempts ago on {latest_success_bi.dt}'

            add_concern(msg)

        else:
            if latest_success_bi.ts - now_unix_ts > 2 * 7 * millis_day:
                # This could be made smarter by recording rebase attempts in the database..
                add_concern(f'Last {url_text(latest_success_bi.task_url, "build")} ({url_text(latest_success_bi.build_url, "jenkins job")}) was over two weeks ago.')

        if not image_concerns:
            del concerns[image_meta.qualified_key]

    # We should now have a dict of qualified_key => [concern, ...]
    if not concerns:
        runtime.logger.info('No concerns to report!')
        return

    print(yaml.dump(concerns, default_flow_style=False, width=10000))
