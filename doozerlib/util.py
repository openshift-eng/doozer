import click


def red_prefix(msg):
    """Print out a message prefix in bold red letters, like for "Error: "
messages"""
    click.secho(msg, nl=False, bold=True, fg='red')


def red_print(msg):
    """Print out a message in red text"
messages"""
    click.secho(msg, nl=True, bold=False, fg='red')


def green_prefix(msg):
    """Print out a message prefix in bold green letters, like for "Success: "
messages"""
    click.secho(msg, nl=False, bold=True, fg='green')


def green_print(msg):
    """Print out a message in green text"""
    click.secho(msg, nl=True, bold=False, fg='green')


def yellow_prefix(msg):
    """Print out a message prefix in bold yellow letters, like for "Success: "
messages"""
    click.secho(msg, nl=False, bold=True, fg='yellow')


def yellow_print(msg):
    """Print out a message in yellow text"""
    click.secho(msg, nl=True, bold=False, fg='yellow')


def cprint(msg):
    """Wrapper for click.echo"""
    click.echo(msg)


def color_print(msg, color='white', nl=True):
    """Print out a message in given color"""
    click.secho(msg, nl=nl, bold=False, fg=color)
