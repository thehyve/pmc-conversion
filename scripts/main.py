import logging
import os
from shutil import copyfile

import luigi

from checksum import calculate_file_checksum_pairs_considering_checksum_files, calculate_file_checksum_pairs
from git_commons import get_git_repo
from luigi_commons import BaseTask, ExternalProgramTask

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

    transmart_copy_jar = luigi.Parameter('Path to transmart copy jar.')
    skinny_dir = luigi.Parameter(description="Skinny loader study directory.")
    study_id = luigi.Parameter(description="Id of the study to load.")
    PGHOST = luigi.Parameter(description="Configuration for transmart-copy.")
    PGPORT = luigi.Parameter(description="Configuration for transmart-copy.")
    PGDATABASE = luigi.Parameter(description="Configuration for transmart-copy.")
    PGUSER = luigi.Parameter(description="Configuration for transmart-copy.")
    PGPASSWORD = luigi.Parameter(description="Configuration for transmart-copy.")

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
repo = get_git_repo(config.repo_root_dir)

os.makedirs(config.input_data_dir, exist_ok=True)
os.makedirs(config.staging_dir, exist_ok=True)
os.makedirs(config.load_logs_dir, exist_ok=True)


class UpdateDataFiles(BaseTask):
    """
    Task to check whether new files are available and copy them over.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.drop_dir_files = None
        self.input_data_dir_dir_files = None

    def run(self):
        logger.info('Differences detected. Start synchronising...')
        remove_files = self.input_data_dir_dir_files - self.drop_dir_files
        add_files = self.drop_dir_files - self.input_data_dir_dir_files

        logger.info(f'Start removing {len(remove_files)} files from the destination directory...')
        for remove_file in remove_files:
            remove_path = os.path.join(config.input_data_dir, remove_file.data_file)
            logger.debug(f'Removing {remove_path} file.')
            os.remove(remove_path)

        logger.info(f'Start copying {len(add_files)} files from the source to destination directory...')
        for add_file in add_files:
            src_path = os.path.join(config.drop_dir, add_file.data_file)
            dst_path = os.path.join(config.input_data_dir, add_file.data_file)
            logger.debug(f'Copying {src_path} file to {dst_path}.')
            os.makedirs(os.path.dirname(dst_path), exist_ok=True)
            copyfile(src_path, dst_path)

    def complete(self):
        logger.info(f'Check source ({config.drop_dir}) directory consistency...')
        self.drop_dir_files = calculate_file_checksum_pairs_considering_checksum_files(config.drop_dir)

        logger.info(f'Reading destination ({config.input_data_dir}) directory content to compare...')
        self.input_data_dir_dir_files = calculate_file_checksum_pairs(config.input_data_dir)

        return self.drop_dir_files == self.input_data_dir_dir_files and os.path.exists(self.done_signal_file)

    def calc_done_signal(self):
        """
        :return: ordered file names with checksums of their content
        """
        sorted_files = sorted(self.input_data_dir_dir_files, key=lambda file: file.data_file)
        return os.linesep.join([file + ' ' + checksum for file, checksum in sorted_files])


update_data_files = UpdateDataFiles()


class GitAddRawFiles(BaseTask):
    """
    Task to add raw data files to git
    """

    def requires(self):
        return update_data_files

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

    wd = '.'

    def program_environment(self):
        os.environ['PGHOST'] = config.PGHOST
        os.environ['PGPORT'] = config.PGPORT
        os.environ['PGDATABASE'] = config.PGDATABASE
        os.environ['PGUSER'] = config.PGUSER
        os.environ['PGPASSWORD'] = config.PGPASSWORD


class DeleteTransmartStudyIfExists(TransmartDataLoader):
    stop_on_error = False

    def requires(self):
        yield GitAddStagingFilesAndCommit()

    def program_args(self):
        return ['java', '-jar', f'{config.transmart_copy_jar!r}', '--delete', f'{config.study_id!r}']


class LoadTransmartStudy(TransmartDataLoader):
    def requires(self):
        yield DeleteTransmartStudyIfExists()

    def program_args(self):
        return ['java', '-jar', f'{config.transmart_copy_jar!r}', '-d', f'{config.skinny_dir!r}']


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
        yield LoadTransmartStudy()
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
        yield LoadTransmartStudy()
        yield DeleteTransmartStudyIfExists()
        yield GitAddStagingFilesAndCommit()
        yield CbioportalDataTransformation()
        yield TransmartDataTransformation()
        yield MergeClinicalData()
        yield GitAddRawFiles()
        yield update_data_files


if __name__ == '__main__':
    luigi.run()
