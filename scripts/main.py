import os

import git
import luigi

from checksum import read_sha1_file
from sync import sync_dirs, is_dirs_in_sync


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
    done_signal_filename = None  # set name here to

    @property
    def done_signal_file(self):
        """ Full path filename that is written to when task is finished successfully. """
        if self.input_signal_file is None:
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


class CheckForNewFiles(BaseTask):
    """
    Task to check whether new files are available
    """

    done_signal_filename = '.done-CheckForNewFiles'

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

    done_signal_filename = '.done-GitAddRawFiles'

    @property
    def input_signal_file(self):
        return self.input()

    def requires(self):
        return CheckForNewFiles()

    def run(self):
        repo.index.add([config.input_data_dir])


class MergeClinicalData(BaseTask):
    """
    Task to merge all clinical data files
    """

    done_signal_filename = '.done-MergeClinicalData'

    @property
    def input_signal_file(self):
        return self.input()

    def requires(self):
        return GitAddRawFiles()

    def run(self):
        pass


class TransmartDataTransformation(BaseTask):
    """
    Task to transform data files for tranSMART
    """

    done_signal_filename = '.done-TransmartDataTransformation'

    @property
    def input_signal_file(self):
        return self.input()

    def requires(self):
        return MergeClinicalData()

    def run(self):
        pass


class CbioportalDataTransformation(BaseTask):
    """
    Task to transform data files for cBioPortal
    """

    done_signal_filename = '.done-CbioportalDataTransformation'

    @property
    def input_signal_file(self):
        return self.input()

    def requires(self):
        return MergeClinicalData()

    def run(self):
        pass


class GitAddStagingFilesAndCommit(BaseTask):
    """
    Task to add transformed files to Git and commit
    """

    done_signal_filename = '.done-GitAddStagingFilesAndCommit'

    @property
    def input_signal_file(self):
        return self.input()

    def requires(self):
        yield TransmartDataTransformation()
        yield CbioportalDataTransformation()

    def run(self):
        repo.index.add([config.staging_dir])
        repo.index.commit(f'Add new input and transformed data.')


class TransmartDataLoader(BaseTask):
    """
    Task to load data to tranSMART
    """

    done_signal_filename = '.done-TransmartDataLoader'

    @property
    def input_signal_file(self):
        return self.input()

    def requires(self):
        return GitAddStagingFilesAndCommit()

    def run(self):
        pass


class CbioportalDataLoader(BaseTask):
    """
    Task to load data to cBioPortal
    """

    done_signal_filename = '.done-CbioportalDataLoader'

    @property
    def input_signal_file(self):
        return self.input()

    def requires(self):
        return GitAddStagingFilesAndCommit()

    def run(self):
        pass


class GitCommitLoadResults(BaseTask):
    """
    Task to amend git commit results with load status
    """

    done_signal_filename = '.done-GitAmendLoadResults'

    @property
    def input_signal_file(self):
        return self.input()

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
        yield CheckForNewFiles()


if __name__ == '__main__':
    luigi.run()
