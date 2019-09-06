import sys
import click
import logging
import smtplib
from configparser import ConfigParser


class Config:
    def __init__(self, config):
        cp = ConfigParser()
        cp.read(config)
        self.receiver = cp.get('email', 'receiver').split(',')
        self.sender = cp.get('email', 'sender')
        self.prefix = cp.get('email', 'prefix')
        self.subject = self.prefix

        self.port = cp.get('smtp', 'port')
        self.host = cp.get('smtp', 'host')
        self.username = cp.get('smtp', 'username')
        self.password = cp.get('smtp', 'password')

        self.template_email = cp.get('global','template_email')
        self.template = self._read_template(self.template_email)
        self.log_file = cp.get('global', 'log_file')
        self.log = self._read_log(self.log_file)

    @staticmethod
    def _read_log(logfile):
        """Open the logfile and read the file as a list with one line per index"""
        with open(logfile, 'r') as fh:
            return fh.readlines()

    @staticmethod
    def _read_template(file):
        """Open the file and read the contents as a string. Raise exceptions when file reading fails"""
        try:
            with open(file, 'r') as fh:
                return fh.read()
        except FileNotFoundError:
            raise FileNotFoundError
        except OSError:
            raise OSError

    def set_subject(self, subject):
        self.subject = '{}{}'.format(self.prefix, subject)


def parse_log_file(log):
    """Loop over the log file and parse the summary and error messages"""
    start,end = '',-1
    errors = []

    for index_, item in enumerate(log):
        if 'Luigi Execution Summary' in item:
            if start == '':
                start = index_
            else:
                end = index_

        elif 'ERROR' in item and 'luigi-interface' not in item:
            errors.append('Line: {} | {}'.format(index_, item))
    summary = log[start+1:end]

    return summary, errors


def sendemail(cp, message):
    """Use the message to send an email to the receiver specified in the config cp"""
    # Construct the payload to send.
    header = build_header(cp)
    payload = header + message

    server = smtplib.SMTP(cp.host)
    server.starttls()
    server.login(cp.username, cp.password)
    try:
        problem = server.sendmail(cp.sender, cp.receiver, payload)
    except Exception as e:
        raise e
    else:
        server.quit()


def build_header(cp):
    """Build the email header for a SMTP email message"""
    header = '\n'.join([
        'From: {}'.format(cp.sender),
        'To: {}'.format(''.join(cp.receiver)),
        'Cc: {}'.format([]),
        'Subject: {}\n\n'.format(cp.subject)
    ])
    return header


def build_message_body(cp, summary, errors):
    """Construct the body of the email to be send based on the summary and errors.
    Uses a template message to insert the summary and errors. The template message should have two format place holders
    named 'summary' and 'errors'.
    """

    return cp.template.format(summary=''.join(summary), errors=''.join(errors), log_file=cp.log_file)


def setup_logger(log_level):
    """Setup logger for the module"""
    logger = logging.getLogger('email_client')
    logger.setLevel(log_level)
    sh = logging.StreamHandler()
    sh.setLevel(log_level)
    format = logging.Formatter('%(asctime)s  %(levelname)-7s %(name)-10s %(message)s')
    sh.setFormatter(format)
    logger.addHandler(sh)
    return logger


@click.command()
@click.option('--config',type=click.Path(exists=True))
@click.option('--log_level',type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']), default='INFO')
def main(config, log_level):
    logger = setup_logger(log_level)

    logger.debug('Reading configuration file')
    cp = Config(config)

    logger.info('Parsing log file {}'.format(cp.log_file))
    summary, errors = parse_log_file(cp.log)

    logger.info('Found {} ERROR message(s)'.format(len(errors)))

    cp.set_subject('ERRORS: {}'.format(len(errors)))

    logger.debug('Constructing message for email')
    message = build_message_body(cp, summary, errors)

    logger.info('Sending email to {}'.format(', '.join(cp.receiver)))
    sendemail(cp, message)

    sys.exit(0)


if __name__ == '__main__':
    main()
