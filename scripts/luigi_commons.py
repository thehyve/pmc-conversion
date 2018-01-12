import logging
import os
import subprocess
import tempfile

import luigi
from luigi.contrib.external_program import ExternalProgramRunContext, ExternalProgramRunError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def read_content(file) -> str:
    with open(file, 'r') as f:
        return f.read()


def signal_files_matches(input_file, output_file):
    if os.path.exists(input_file) and os.path.exists(output_file):
        shain = read_content(input_file)
        shaout = read_content(output_file)
        match = shain == shaout
        logger.debug('These files match: {} \t{}, {}'.format(match, input_file, output_file))
        return match
    return False


class BaseTask(luigi.Task):
    """
    Provides the basis for a task based on a input_signal_file with a hash identifier
    and a done_signal_file. A task is considered completed when the input signal is identical
    to the done signal.

    Most tasks only have to set the input and done signal file attributes and
    define a requires and run method.
    """

    input_signal_file = None  # has to be set as full path.

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done_signal_filename = '.done-{}'.format(self.__class__.__name__)
        self.required_tasks = []

    def requires(self):
        return self.required_tasks

    @property
    def input_signal_file(self):
        return self.input()

    @property
    def done_signal_file(self):
        """ Full path filename that is written to when task is finished successfully. """
        if not self.input_signal_file:
            return os.path.join(self.done_signal_filename)

        if isinstance(self.input_signal_file, list):
            return os.path.join(os.path.dirname(self.input_signal_file[0]), self.done_signal_filename)
        else:
            return os.path.join(os.path.dirname(self.input_signal_file), self.done_signal_filename)

    def complete(self):
        """
        By default a task is complete when the input_signal_file identifier is the
        same as the done_signal_file identifier.
        """
        if isinstance(self.input_signal_file, list):
            for input_signal_file in self.input_signal_file:
                if not signal_files_matches(input_signal_file, self.done_signal_file):
                    return False
            return os.path.exists(self.done_signal_file)
        else:
            return signal_files_matches(self.done_signal_file, self.input_signal_file)

    def calc_done_signal(self):
        """
        Where should the task get the identifier to be written to self.done_signal_file.
        Default is the same as the input. Feel free to override this method.

        :return: some identifier (sha1 hash)
        """
        if isinstance(self.input_signal_file, list):
            input_signal_file = self.input_signal_file[0]
        else:
            input_signal_file = self.input_signal_file

        with open(input_signal_file, 'r') as f:
            return f.read()

    def output(self):
        """ Send the done signal file to the tasks that requires it. """
        return self.done_signal_file

    def on_success(self):
        """ Write the done signal once the task is finished successfully. """

        with open(self.done_signal_file, 'w') as f:
            f.write(self.calc_done_signal())


class ExternalProgramTask(BaseTask):
    """
    Template task for running an external program in a subprocess

    The program is run using :py:class:`subprocess.Popen`, with ``args`` passed
    as a list, generated by :py:meth:`program_args` (where the first element should
    be the executable). See :py:class:`subprocess.Popen` for details.

    Your must override :py:meth:`program_args` to specify the arguments you want,
    and you can optionally override :py:meth:`program_environment` if you want to
    control the environment variables (see :py:class:`ExternalPythonProgramTask`
    for an example).
    """

    stop_on_error = True

    def program_args(self):
        """
        Override this method to map your task parameters to the program arguments

        :return: list to pass as ``args`` to :py:class:`subprocess.Popen`
        """
        raise NotImplementedError

    def program_environment(self):
        """
        Override this method to control environment variables for the program

        :return: dict mapping environment variable names to values
        """
        env = os.environ.copy()
        return env

    @property
    def always_log_stderr(self):
        """
        When True, stderr will be logged even if program execution succeeded

        Override to False to log stderr only when program execution fails.
        """
        return True

    def _clean_output_file(self, file_object):
        file_object.seek(0)
        return ''.join(map(lambda s: s.decode('utf-8'), file_object.readlines()))

    def run(self):
        args = list(map(str, self.program_args()))

        logger.info('Running command: %s', ' '.join(args))
        tmp_stdout, tmp_stderr = tempfile.TemporaryFile(), tempfile.TemporaryFile()
        env = self.program_environment()
        proc = subprocess.Popen(
            ' '.join(args),
            env=env,
            cwd=self.wd,
            stdout=tmp_stdout,
            stderr=tmp_stderr,
            shell=True
        )

        try:
            with ExternalProgramRunContext(proc):
                proc.wait()
            success = proc.returncode == 0

            stdout = self._clean_output_file(tmp_stdout)
            stderr = self._clean_output_file(tmp_stderr)

            if stdout:
                logger.info('Program stdout:\n{}'.format(stdout))
            if stderr:
                if self.always_log_stderr or not success:
                    logger.info('Program stderr:\n{}'.format(stderr))

            if not success and self.stop_on_error:
                raise ExternalProgramRunError(
                    'Program failed with return code={}:'.format(proc.returncode),
                    args, env=env, stdout=stdout, stderr=stderr)
        finally:
            tmp_stderr.close()
            tmp_stdout.close()
