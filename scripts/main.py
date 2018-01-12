import logging
import os

import luigi
from .git_commons import get_git_repo
from .sync import sync_dirs, is_dirs_in_sync
from .luigi_commons import BaseTask, ExternalProgramTask
from .codebook_formatting import codebook_formatting

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

    transmart_copy_jar = luigi.Parameter(description='Path to transmart copy jar.')
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
    Task to check whether new files are available
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_modifications = None

    def run(self):
        self.file_modifications = sync_dirs(config.drop_dir, config.input_data_dir)

    def complete(self):
        return is_dirs_in_sync(config.drop_dir, config.input_data_dir) and os.path.exists(self.done_signal_file)

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


class FormatCodeBooks(BaseTask):

    cm_map = '/Users/wibopipping/Projects/PMC/pmc-conversion/config/codebook_mapping.json'
    output_dir = '/Users/wibopipping/Projects/PMC/csr_luigi_pipeline/intermediate'

    def requires(self):
        yield GitAddRawFiles()

    def run(self):
        for path, dir_, filenames in os.walk(config.input_data_dir):
            codebooks = [file for file in filenames if 'codebook' in file]
            for codebook in codebooks:
                codebook_file = os.path.join(path,codebook)
                codebook_formatting(codebook_file, self.cm_map, self.output_dir)


class MergeClinicalData(ExternalProgramTask):
    wd = luigi.Parameter(description='Working directory with the CSR transformation script', significant=False)
    csr_transformation = luigi.Parameter(description='CSR transformation script name', significant=False)
    csr_config = luigi.Parameter(description='CSR transformation config file', significant=False)
    python_version = luigi.Parameter(description='Python command to use to execute', significant=False)
    config_dir = luigi.Parameter(description='CSR transformation config dir', significant=False)

    def requires(self):
        yield FormatCodeBooks()
        # return GitAddRawFiles()

    def program_args(self):
        return [self.python_version, self.csr_transformation, self.csr_config, '--config_dir', self.config_dir]


class TransmartDataTransformation(ExternalProgramTask):
    wd = luigi.Parameter(description='Working directory with the tranSMART transformation script', significant=False)
    tm_transformation = luigi.Parameter(description='tranSMART data transformation script name', significant=False)
    tm_config = luigi.Parameter(description='tranSMART data transformation config file', significant=False)
    python_version = luigi.Parameter(description='Python command to use to execute', significant=False)
    config_dir = luigi.Parameter(description='tranSMART transformation config dir', significant=False)

    def requires(self):
        yield MergeClinicalData()

    def program_args(self):
        return [self.python_version, self.tm_transformation, self.tm_config, '--config_dir', self.config_dir]


class CbioportalDataTransformation(ExternalProgramTask):
    """
    Task to transform data files for cBioPortal
    """

    def requires(self):
        return MergeClinicalData()

    wd = '.'
    cbio_transformation_clinical_input_file = luigi.Parameter(description='cBioPortal clinical input file', significant=False)
    cbio_transformation_ngs_dir = luigi.Parameter(description='cBioPortal NGS file directory', significant=False)
    cbio_transformation_output_dir = luigi.Parameter(description='cBioPortal output directory', significant=False)

    def program_args(self):
        return ['python3 cbioportal_transformation/pmc_cbio_wrapper.py',
                '-c', self.cbio_transformation_clinical_input_file,
                '-n', self.cbio_transformation_ngs_dir,
                '-o', self.cbio_transformation_output_dir]

class GitAddStagingFilesAndCommit(BaseTask):
    """
    Task to add transformed files to Git and commit
    """

    def requires(self):
        yield TransmartDataTransformation()
        yield CbioportalDataTransformation()

    def run(self):
        repo.index.add([config.staging_dir])
        repo.index.commit('Add new input and transformed data.')


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
        return ['java', '-jar', '{!r}'.format(config.transmart_copy_jar), '--delete', '{!r}'.format(config.study_id)]


class LoadTransmartStudy(TransmartDataLoader):
    def requires(self):
        yield DeleteTransmartStudyIfExists()

    def program_args(self):
        return ['java', '-jar', '{!r}'.format(config.transmart_copy_jar), '-d', '{!r}'.format(config.skinny_dir)]


class CbioportalDataLoader(ExternalProgramTask):
    """
    Task to load data to cBioPortal
    """
    def requires(self):
        return GitAddStagingFilesAndCommit()

    # # Variables for importer
    # cbio_loader_input_dir = luigi.Parameter(description='cBioPortal staging file directory', significant=False)
    # cbio_loader_docker_image = luigi.Parameter(description='cBioPortal docker image', significant=False)
    # 
    # # Variables for validation
    # cbio_loader_report_dir = luigi.Parameter(description='Validation report output directory', significant=False)
    # cbio_loader_report_name = luigi.Parameter(description='Validation report name', significant=False)
    # cbio_loader_portal_url = luigi.Parameter(description='URL to cBioPortal server', significant=False)

    # This requires:
    # 1. Docker installed
    # 2. cbioportal image in Docker
    # 3. A running cBioPortal instance
    # 4. A running cBioPortal database
    # def program_args(self):
    #     # Build the command for importer only
    #     cbio_loader_docker_command = 'docker run --rm --net=cbio-net -v %s:/study/ %s' \
    #                                  % (self.cbio_loader_input_dir, self.cbio_loader_docker_image)
    #     cbio_loader_python_command = 'python ' \
    #                                  '/cbioportal/core/src/main/scripts/importer/cbioportalImporter.py -s /study/'
    #
    #     # Build the command for validation and importer. TODO: Use this when we receive valid data
    #     # cbio_loader_docker_command = 'docker run --rm --net=cbio-net -v %s:/study/ -v %s:/html_reports/ %s' \
    #     #                              % (self.cbio_loader_input_dir, self.cbio_loader_report_dir,
    #     #                                 self.cbio_loader_docker_image)
    #     # cbio_loader_python_command = 'python ' \
    #     #                              '/cbioportal/core/src/main/scripts/importer/metaImport.py -s /study/ -u %s ' \
    #     #                              '-html /html_reports/%s.html -v -o ' \
    #     #                              % (self.cbio_loader_portal_url, self.cbio_loader_report_name)
    #     # return [cbio_loader_docker_command, cbio_loader_python_command]

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
        repo.index.commit('Add load results.')


class DataLoader(luigi.WrapperTask):
    """
    Wrapper task whose purpose it is to check whether any tasks need be rerun
    to perform an update and then do it.
    """

    def requires(self):
        yield UpdateDataFiles()
        yield GitAddRawFiles()
        yield FormatCodeBooks()
        yield MergeClinicalData()
        yield TransmartDataTransformation()
        yield CbioportalDataTransformation()
        yield GitAddStagingFilesAndCommit()
        yield DeleteTransmartStudyIfExists()
        yield LoadTransmartStudy()
        yield CbioportalDataLoader()
        yield GitCommitLoadResults()


if __name__ == '__main__':
    luigi.run()
