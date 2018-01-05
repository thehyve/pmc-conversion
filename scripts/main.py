import logging
import os
import subprocess

import git
import luigi

from luigi.contrib.external_program import ExternalProgramRunContext, ExternalProgramRunError
import tempfile

from checksum import read_sha1_file
from sync import sync_dirs, is_dirs_in_sync

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class GlobalConfig(luigi.Config):
    drop_dir = luigi.Parameter(description='Directory files gets uploaded to.')

    repo_root_dir = luigi.Parameter(description='Path to the git repository.')
    input_data_dir_name = luigi.Parameter(description='Original provided files under the repository.',
                                          default='input_data')
    staging_dir_name = luigi.Parameter(description='Directory where ready to load transformed files are stored.',
                                       default='staging')
    load_logs_dir_name = luigi.Parameter(description='Path to the log files of the loading scripts.',
                                         default='load_logs')

    @property
    def input_data_dir(self):
        return os.path.join(self.repo_root_dir, self.input_data_dir_name)

    @property
    def staging_dir(self):
        return os.path.join(self.repo_root_dir, self.staging_dir_name)

    @property
    def load_logs_dir(self):
        return os.path.join(self.repo_root_dir, self.load_logs_dir_name)


config = GlobalConfig()


def get_git_repo(repo_dir):
    """
    Returns the git repository used for VCS of source and transformed data files. As well as load logs.
    If it does not exist, it will create one.

    :return: git.Repo
    """
    if not os.path.exists(repo_dir):
        return init_git_repo(repo_dir)

    try:
        return git.Repo(repo_dir)
    except git.InvalidGitRepositoryError:
        return init_git_repo(config.repo_root_dir)


def init_git_repo(repo_dir):
    os.makedirs(config.repo_root_dir, exist_ok=True)
    print(f'Initializing git repository: {repo_dir}')
    r = git.Repo.init(os.path.realpath(repo_dir))
    ignore_list = ['.done-*', '.DS_Store']

    gitignore = os.path.realpath(os.path.join(repo_dir, '.gitignore'))

    with open(gitignore, 'w') as f:
        f.write('\n'.join(ignore_list))

    r.index.add([gitignore])
    r.index.commit('Initial commit.')
    return r


repo = get_git_repo(config.repo_root_dir)


def signal_files_matches(input_file, output_file):
    if os.path.exists(input_file) and os.path.exists(output_file):
        return read_sha1_file(input_file) == read_sha1_file(output_file)
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
        self.done_signal_filename = f'.done-{self.__class__.__name__}'

    @property
    def input_signal_file(self):
        return self.input()

    @property
    def done_signal_file(self):
        """ Full path filename that is written to when task is finished successfully. """
        if not self.input_signal_file:
            return self.done_signal_filename

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
            return True
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

            if not success:
                raise ExternalProgramRunError(
                    'Program failed with return code={}:'.format(proc.returncode),
                    args, env=env, stdout=stdout, stderr=stderr)
        finally:
            tmp_stderr.close()
            tmp_stdout.close()


class UpdateDataFiles(BaseTask):
    """
    Task to check whether new files are available
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_modifications = None

    def run(self):
        self.file_modifications = sync_dirs(config.drop_dir, config.input_data_dir)

    def complete(self):
        return is_dirs_in_sync(config.drop_dir, config.input_data_dir)

    def calc_done_signal(self):
        """
        :return: file with list of files and their checksums
        """
        removed_files = sorted(self.file_modifications.removed, key=lambda removed_file: removed_file.data_file)
        added_files = sorted(self.file_modifications.added, key=lambda added_file: added_file.data_file)
        return os.linesep.join([' - ' + file + ' ' + checksum for file, checksum in removed_files] +
                               [' + ' + file + ' ' + checksum for file, checksum in added_files])


class GitAddRawFiles(BaseTask):
    """
    Task to add raw data files to git
    """

    def requires(self):
        return UpdateDataFiles()

    def run(self):
        repo.index.add([config.input_data_dir])


class SubprocessException(Exception):
    pass


class MergeClinicalData(ExternalProgramTask):

    wd = luigi.Parameter('Working directory with the CSR transformation script', significant=False)
    csr_transformation = luigi.Parameter('CSR transformation script name', significant=False)
    csr_config = luigi.Parameter('CSR transformation config file', significant=False)
    python_version = luigi.Parameter('Python command to use to execute', significant=False)

    def requires(self):
        return GitAddRawFiles()

    def program_args(self):
        return [self.python_version, self.csr_transformation, self.csr_config]


class TransmartDataTransformation(ExternalProgramTask):

    wd = luigi.Parameter('Working directory with the tranSMART transformation script', significant=False)
    tm_transformation = luigi.Parameter('tranSMART data transformation script name', significant=False)
    tm_config = luigi.Parameter('tranSMART data transformation config file', significant=False)
    python_version = luigi.Parameter('Python command to use to execute', significant=False)

    def requires(self):
        yield MergeClinicalData()

    def program_args(self):
        return [self.python_version, self.tm_transformation, self.tm_config]


class CbioportalDataTransformation(BaseTask):
    """
    Task to transform data files for cBioPortal
    """

    def requires(self):
        return MergeClinicalData()

    def run(self):
        pass


class GitAddStagingFilesAndCommit(BaseTask):
    """
    Task to add transformed files to Git and commit
    """

    def requires(self):
        yield TransmartDataTransformation()
        yield CbioportalDataTransformation()

    def run(self):
        repo.index.add([config.staging_dir])
        repo.index.commit(f'Add new input and transformed data.')


class TransmartDataLoader(ExternalProgramTask):
    """
    Task to load data to tranSMART
    """

    transmart_copy_jar = luigi.Parameter('Path to transmart copy jar.')
    skinny_dir = luigi.Parameter(description="Skinny loader study directory.")
    study_id = luigi.Parameter(description="Id of the study to load.")
    PGHOST = luigi.Parameter(description="Configuration for transmart-copy.")
    PGPORT = luigi.Parameter(description="Configuration for transmart-copy.")
    PGDATABASE = luigi.Parameter(description="Configuration for transmart-copy.")
    PGUSER = luigi.Parameter(description="Configuration for transmart-copy.")
    PGPASSWORD = luigi.Parameter(description="Configuration for transmart-copy.")
    wd = '.'

    @property
    def input_signal_file(self):
        return self.input()

    def requires(self):
        return GitAddStagingFilesAndCommit()

    def program_args(self):
        return ['java', '-jar', f'{self.transmart_copy_jar!r}', '-d', f'{self.skinny_dir!r}']

    def program_environment(self):
        os.environ['PGHOST'] = self.PGHOST
        os.environ['PGPORT'] = self.PGPORT
        os.environ['PGDATABASE'] = self.PGDATABASE
        os.environ['PGUSER'] = self.PGUSER
        os.environ['PGPASSWORD'] = self.PGPASSWORD


class CbioportalDataLoader(BaseTask):
    """
    Task to load data to cBioPortal
    """

    def requires(self):
        return GitAddStagingFilesAndCommit()

    def run(self):
        pass


class GitCommitLoadResults(BaseTask):
    """
    Task to amend git commit results with load status
    """

    def requires(self):
        yield TransmartDataLoader()
        yield CbioportalDataLoader()

    def run(self):
        repo.index.add([config.load_logs_dir])
        repo.index.commit(f'Add load results.')


class DataLoader(luigi.WrapperTask):
    """
    Wrapper task whose purpose it is to check whether any tasks need be rerun
    to perform an update and then do it.
    """

    def requires(self):
        yield GitCommitLoadResults()
        yield CbioportalDataLoader()
        yield TransmartDataLoader()
        yield GitAddStagingFilesAndCommit()
        yield CbioportalDataTransformation()
        yield TransmartDataTransformation()
        yield MergeClinicalData()
        yield GitAddRawFiles()
        yield UpdateDataFiles()


if __name__ == '__main__':
    luigi.run()
