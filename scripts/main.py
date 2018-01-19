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
    intermediate_file_dir = luigi.Parameter(description='Path to the repo to store the intermediate data products')

    config_json_dir = luigi.Parameter(description='Folder with mapping files in JSON format')

    python = luigi.Parameter(description='Python version to use when executing ExternalProgram Tasks',
                             default='python3')

    csr_data_file = luigi.Parameter(description='Combined clinical data for the Central subject registry')

    transmart_copy_jar = luigi.Parameter(description='Path to transmart copy jar.')
    study_id = luigi.Parameter(description="Id of the study to load.")
    top_node = luigi.Parameter(description='Topnode of the study to load')
    security_required = luigi.Parameter(description='Either True or False, TranSMART study should be private?')

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
os.makedirs(config.intermediate_file_dir, exist_ok=True)


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

    def requires(self):
        yield GitAddRawFiles()

    def run(self):
        for path, dir_, filenames in os.walk(config.input_data_dir):
            codebooks = [file for file in filenames if 'codebook' in file]
            for codebook in codebooks:
                codebook_file = os.path.join(path, codebook)
                codebook_formatting(codebook_file, self.cm_map, config.intermediate_file_dir)


class MergeClinicalData(ExternalProgramTask):

    wd = os.path.join(os.getcwd(), 'scripts')

    csr_transformation = luigi.Parameter(description='CSR transformation script name', significant=False)
    data_model = luigi.Parameter(description='JSON file with the columns per entity', significant=False)
    file_list = luigi.Parameter(description='Flat text file with an ordered list of expected files', significant=False)
    file_headers = luigi.Parameter(description='JSON file with a list of columns per data file', significant=False,
                                   default=None)
    columns_to_csr = luigi.Parameter(
        description='Data file columns mapped to expected fields in the central subject registry', significant=False)

    def requires(self):
        yield FormatCodeBooks()

    def program_args(self):
        if self.file_headers:
            return [config.python, self.csr_transformation,
                '--input_dir', config.input_data_dir,
                '--output_dir', config.intermediate_file_dir,
                '--config_dir', config.config_json_dir,
                '--data_model', self.data_model,
                '--file_list', self.file_list,
                '--columns_to_csr', self.columns_to_csr,
                '--output_filename', config.csr_data_file,
                '--file_headers', self.file_headers]
        else:
            return [config.python, self.csr_transformation,
                '--input_dir', config.input_data_dir,
                '--output_dir', config.intermediate_file_dir,
                '--config_dir', config.config_json_dir,
                '--data_model', self.data_model,
                '--file_list', self.file_list,
                '--columns_to_csr', self.columns_to_csr,
                '--output_filename', self.output_filename]


class TransmartDataTransformation(ExternalProgramTask):

    wd = os.path.join(os.getcwd(), 'scripts')

    tm_transformation = luigi.Parameter(description='tranSMART data transformation script name', significant=False)
    blueprint = luigi.Parameter(description='Blueprint file to map the data to the tranSMART ontology')
    modifiers = luigi.Parameter(description='Modifiers used by tranSMART')

    def requires(self):
        yield MergeClinicalData()

    def program_args(self):
        return [config.python, self.tm_transformation,
                '--csr_data_file', os.path.join(config.intermediate_file_dir, config.csr_data_file),
                '--output_dir', config.staging_dir,
                '--config_dir', config.config_json_dir,
                '--blueprint', self.blueprint,
                '--modifiers', self.modifiers,
                '--study_id', config.study_id,
                '--top_node', '{!r}'.format(config.top_node),
                '--security_required',config.security_required]


class CbioportalDataTransformation(ExternalProgramTask):
    """
    Task to transform data files for cBioPortal
    """

    # def requires(self):
    #     return MergeClinicalData()

    clinical_input_file = luigi.Parameter(description='cBioPortal clinical input file', significant=False)
    ngs_dir = luigi.Parameter(description='cBioPortal NGS file directory', significant=False)
    output_dir = luigi.Parameter(description='cBioPortal output directory', significant=False)

    def program_args(self):
        return ['python3 cbioportal_transformation/pmc_cbio_wrapper.py',
                '-c', self.clinical_input_file,
                '-n', self.ngs_dir,
                '-o', self.output_dir]


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
        return ['java', '-jar', '{!r}'.format(config.transmart_copy_jar), '-d', '{!r}'.format(config.staging_dir)]


class CbioportalDataValidation(ExternalProgramTask):
    """
    Task to validate data for cBioPortal

    This requires:
    1. Docker installed
    2. cBioPortal image in Docker
    3. pmc user added to group 'docker'
    4. A running cBioPortal instance
    5. A running cBioPortal database
    """
    def requires(self):
        return GitAddStagingFilesAndCommit()

    # Variables for importer
    input_dir = luigi.Parameter(description='cBioPortal staging file directory', significant=False)
    docker_image = luigi.Parameter(description='cBioPortal docker image', significant=False)

    # Variables for validation
    report_dir = luigi.Parameter(description='Validation report output directory', significant=False)
    db_info_dir = luigi.Parameter(description='cBioPortal info directory', significant=False)
    report_name = luigi.Parameter(description='Validation report name', significant=False)

    def program_args(self):
        # Build the command for validation
        docker_command = 'docker run --rm --net=cbio-net -v %s:/study/ -v /etc/hosts:/etc/hosts ' \
                         '-v %s:/cbioportal_db_info/ -v %s:/html_reports/ %s' \
                         % (self.input_dir, self.db_info_dir, self.report_dir, self.docker_image)

        python_command = 'python /cbioportal/core/src/main/scripts/importer/validateData.py -s /study/ ' \
                         '-P /cbioportal/src/main/resources/portal.properties ' \
                         '-p /cbioportal_db_info -html /html_reports/%s.html -v' \
                         % self.report_name
        return [docker_command, python_command]


class CbioportalDataLoading(ExternalProgramTask):
    """
    Task to load data to cBioPortal

    This requires:
    1. Docker installed
    2. cBioPortal image in Docker
    3. pmc user added to group 'docker'
    4. A running cBioPortal instance
    5. A running cBioPortal database
    """

    def requires(self):
        return CbioportalDataValidation()

    # Variables for importer
    input_dir = luigi.Parameter(description='cBioPortal staging file directory', significant=False)
    docker_image = luigi.Parameter(description='cBioPortal docker image', significant=False)

    def program_args(self):
        # Build the command for importer only
        docker_command = 'docker run --rm --net=cbio-net -v %s:/study/ -v /etc/hosts:/etc/hosts %s' \
                         % (self.input_dir, self.docker_image)
        python_command = 'python /cbioportal/core/src/main/scripts/importer/cbioportalImporter.py -s /study/'

        # This should be an ssh command to restart the cBioPortal container on the cBioPortal server
        restart_command = ''
        return [docker_command, python_command, restart_command]


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
        yield CbioportalDataValidation()
        yield CbioportalDataLoading()
        yield GitCommitLoadResults()


if __name__ == '__main__':
    luigi.run()
