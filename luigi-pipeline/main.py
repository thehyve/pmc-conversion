import logging
import os

import luigi
import time
import threading

from .luigi_commons import BaseTask, ExternalProgramTask

from scripts.git_commons import get_git_repo
from scripts.sync import sync_dirs, get_checksum_pairs_set
from scripts.codebook_formatting import codebook_formatting
from scripts.csr_transformations import csr_transformation
from scripts.transmart_api_calls import TransmartApiCalls
from scripts.cbioportal_transformation.cbio_wrapper import create_cbio_study

logger = logging.getLogger('luigi')

TRANSMART_DIR_NAME = 'transmart'
CBIOPORTAL_DIR_NAME = 'cbioportal'


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
    study_registry_file = luigi.Parameter(description='Combined study and individual_study entity'
                                                      'data for the Central subject registry')

    transmart_copy_jar = luigi.Parameter(description='Path to transmart copy jar.')
    study_id = luigi.Parameter(description="Id of the study to load.")
    top_node = luigi.Parameter(description='Topnode of the study to load')
    security_required = luigi.Parameter(description='Either True or False, TranSMART study should be private?')

    PGHOST = luigi.Parameter(description="Configuration for transmart-copy.")
    PGPORT = luigi.Parameter(description="Configuration for transmart-copy.")
    PGDATABASE = luigi.Parameter(description="Configuration for transmart-copy.")
    PGUSER = luigi.Parameter(description="Configuration for transmart-copy.", significant=True)
    PGPASSWORD = luigi.Parameter(description="Configuration for transmart-copy.", significant=False)

    @property
    def input_data_dir(self):
        return os.path.join(self.repo_root_dir, self.input_data_dir_name)

    @property
    def transmart_staging_dir(self):
        return os.path.join(self.repo_root_dir, self.staging_dir_name, TRANSMART_DIR_NAME)

    @property
    def load_logs_dir(self):
        return os.path.join(self.repo_root_dir, self.load_logs_dir_name)

    @property
    def cbioportal_staging_dir(self):
        return os.path.join(self.repo_root_dir, self.staging_dir_name, CBIOPORTAL_DIR_NAME)

    @property
    def transmart_load_logs_dir(self):
        return os.path.join(self.load_logs_dir, TRANSMART_DIR_NAME)

    @property
    def cbioportal_load_logs_dir(self):
        return os.path.join(self.load_logs_dir, CBIOPORTAL_DIR_NAME)


config = GlobalConfig()
git_lock = threading.RLock()
repo = get_git_repo(config.repo_root_dir)

os.makedirs(config.input_data_dir, exist_ok=True)
os.makedirs(config.cbioportal_staging_dir, exist_ok=True)
os.makedirs(config.transmart_staging_dir, exist_ok=True)
os.makedirs(config.intermediate_file_dir, exist_ok=True)
os.makedirs(config.transmart_load_logs_dir, exist_ok=True)
os.makedirs(config.cbioportal_load_logs_dir, exist_ok=True)


def calc_done_signal_content(file_checksum_pairs):
    srt_file_checksum_pairs = sorted(file_checksum_pairs, key=lambda file_checksum_pair: file_checksum_pair.data_file)
    return '\n'.join([file + ' ' + checksum for file, checksum in srt_file_checksum_pairs])


class GitCommit(BaseTask):
    directory_to_add = luigi.Parameter(description='Directory content of which to commit.')
    commit_message = luigi.Parameter(description='Commit message.')

    def run(self):
        with git_lock:
            repo.index.add([self.directory_to_add])
            if len(repo.index.diff('HEAD')):
                repo.index.commit(self.commit_message)
                logger.info('Commit changes in {} directory.'.format(self.directory_to_add))
            else:
                logger.info('Skip commit as there are no changes in {} directory.'.format(self.directory_to_add))


class UpdateDataFiles(BaseTask):
    """
    Task to check whether new files are available
    """

    file_modifications = None

    def run(self):
        self.file_modifications = sync_dirs(config.drop_dir, config.input_data_dir)

    def complete(self):
        return self.file_modifications is not None

    def calc_done_signal(self):
        """
        :return: file with list of files and their checksums
        """
        return calc_done_signal_content(self.file_modifications.new_files)


class FormatCodeBooks(BaseTask):
    cm_map_file = luigi.Parameter(description='Codebook mapping file', significant=False)

    def run(self):
        cm_map = os.path.join(config.config_json_dir, self.cm_map_file)

        for path, dir_, filenames in os.walk(config.input_data_dir):
            codebooks = [file for file in filenames if 'codebook' in file]
            for codebook in codebooks:
                codebook_file = os.path.join(path, codebook)
                codebook_formatting(codebook_file, cm_map, config.intermediate_file_dir)


class MergeClinicalData(BaseTask):
    data_model = luigi.Parameter(description='JSON file with the columns per entity', significant=False)
    column_priority = luigi.Parameter(description='Flat text file with an ordered list of expected files',
                                      significant=False)
    file_headers = luigi.Parameter(description='JSON file with a list of columns per data file', significant=False)
    columns_to_csr = luigi.Parameter(
        description='Data file columns mapped to expected fields in the central subject registry', significant=False)

    def run(self):
        csr_transformation(input_dir=config.input_data_dir,
                           output_dir=config.intermediate_file_dir,
                           config_dir=config.config_json_dir,
                           data_model=self.data_model,
                           column_priority=self.column_priority,
                           file_headers=self.file_headers,
                           columns_to_csr=self.columns_to_csr,
                           output_filename=config.csr_data_file,
                           output_study_filename=config.study_registry_file)


class TransmartDataTransformation(ExternalProgramTask):
    wd = os.path.join(os.getcwd(), 'scripts')

    tm_transformation = luigi.Parameter(description='tranSMART data transformation script name', significant=False)
    blueprint = luigi.Parameter(description='Blueprint file to map the data to the tranSMART ontology')
    modifiers = luigi.Parameter(description='Modifiers used by tranSMART')

    std_out_err_dir = os.path.join(config.transmart_load_logs_dir, 'transformations')

    def program_args(self):
        return [config.python, self.tm_transformation,
                '--csr_data_file', os.path.join(config.intermediate_file_dir, config.csr_data_file),
                '--study_registry_data_file', os.path.join(config.intermediate_file_dir, config.study_registry_file),
                '--output_dir', config.transmart_staging_dir,
                '--config_dir', config.config_json_dir,
                '--blueprint', self.blueprint,
                '--modifiers', self.modifiers,
                '--study_id', config.study_id,
                '--top_node', '{!r}'.format(config.top_node),
                '--security_required', config.security_required]


class CbioportalDataTransformation(BaseTask):
    """
    Task to transform data files for cBioPortal
    """

    cbioportal_header_descriptions = luigi.Parameter(description='JSON file with a description per column')

    # Get NGS dir
    for dir, dirs, files in os.walk(config.input_data_dir):
        if 'NGS' in dirs:
            ngs_dir = os.path.join(dir, dirs[dirs.index('NGS')])
            logger.info('Found NGS data directory: {}'.format(ngs_dir))
            break

    def run(self):
        clinical_input_file = os.path.join(config.intermediate_file_dir, config.csr_data_file)
        description_mapping = os.path.join(config.config_json_dir, self.cbioportal_header_descriptions)

        create_cbio_study(clinical_input_file=clinical_input_file,
                          ngs_dir=self.ngs_dir,
                          output_dir=config.cbioportal_staging_dir,
                          descriptions=description_mapping)


class TransmartDataLoader(ExternalProgramTask):
    """
    Task to load data to tranSMART
    """

    wd = '.'
    std_out_err_dir = os.path.join(config.transmart_load_logs_dir, 'loader')

    def program_environment(self):
        os.environ['PGHOST'] = config.PGHOST
        os.environ['PGPORT'] = config.PGPORT
        os.environ['PGDATABASE'] = config.PGDATABASE
        os.environ['PGUSER'] = config.PGUSER
        os.environ['PGPASSWORD'] = config.PGPASSWORD

    def program_args(self):
        return ['java', '-jar', '{!r}'.format(config.transmart_copy_jar), '--directory',
                '{!r}'.format(config.transmart_staging_dir)]


class TransmartApiTask(BaseTask):
    keycloak_url = luigi.Parameter(description='URL of the keycloak instance, include the realm in the URL')
    client_id = luigi.Parameter(description='client_id of transmart client configured in keycloak')
    client_secret = luigi.Parameter(description='client_secret of transmart client configured in keycloak')
    transmart_url = luigi.Parameter(description='URL of the tranSMART instance', significant=False)
    transmart_username = luigi.Parameter(description='Username for an admin account', significant=False)
    transmart_password = luigi.Parameter(description='Password for the admin account', significant=False)

    def run(self):
        reload_obj = TransmartApiCalls(keycloak_url=self.keycloak_url,
                                       username=self.transmart_username,
                                       password=self.transmart_password,
                                       transmart_url=self.transmart_url,
                                       client_id=self.client_id,
                                       client_secret=self.client_secret)

        # logger.info('Clearing tree cache')
        # reload_obj.clear_tree_nodes_cache()
        # logger.info('Rebuilding tree cache')
        # reload_obj.rebuild_tree_cache()
        # logger.info('Scanning for new subscriptions')
        # reload_obj.scan_subscription_queries()
        logger.info('After data loading update; clearing caches and scanning query subscriptions')
        reload_obj.after_data_loading()


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

    # Set specific docker image
    docker_image = luigi.Parameter(description='cBioPortal docker image', significant=False)
    std_out_err_dir = os.path.join(config.cbioportal_load_logs_dir, 'validation')

    # Success codes for validation
    success_codes = [0, 3]

    def program_args(self):
        # Directory and file names for validation
        input_dir = config.cbioportal_staging_dir
        report_dir = config.cbioportal_load_logs_dir
        db_info_dir = os.path.join(config.config_json_dir, 'cbioportal_db_info')
        report_name = 'report_pmc_test_%s.html' % time.strftime("%Y%m%d-%H%M%S")

        # Build validation command. No connection has to be made to the database or web server.
        docker_command = 'docker run --rm -v %s:/study/ -v %s:/cbioportal_db_info/ -v %s:/html_reports/ %s' \
                         % (input_dir, db_info_dir, report_dir, self.docker_image)

        python_command = 'python /cbioportal/core/src/main/scripts/importer/validateData.py -s /study/ ' \
                         '-P /cbioportal/src/main/resources/portal.properties ' \
                         '-p /cbioportal_db_info -html /html_reports/%s -v' \
                         % report_name
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
    std_out_err_dir = os.path.join(config.cbioportal_load_logs_dir, 'loader')

    # Variables
    docker_image = luigi.Parameter(description='cBioPortal docker image', significant=False)
    server_name = luigi.Parameter(description='Server on which pipeline is running. If running docker locally, leave '
                                              'empty. PMC servers: pmc-cbioportal-test | '
                                              'pmc-cbioportal-acc | pmc-cbioportal-prod', significant=False)

    def program_args(self):
        # Directory and file names for validation
        input_dir = config.cbioportal_staging_dir

        python_command = 'python /cbioportal/core/src/main/scripts/importer/cbioportalImporter.py -s /study/'

        # Check if cBioPortal is running locally or on other server
        if self.server_name == "":
            # Build import command for running the pipeline locally
            docker_command = 'docker run --rm -v %s:/study/ --net cbio-net %s' \
                             % (input_dir, self.docker_image)

            # Restart cBioPortal web server docker container on the local machine
            restart_command = "; docker restart cbioportal"
        else:
            # Build the import command for running the pipeline on the PMC staging server
            docker_command = 'docker run --network="host" --rm -v %s:/study/ -v /etc/hosts:/etc/hosts %s' \
                             % (input_dir, self.docker_image)

            # Restart cBioPortal web server docker container which runs on a different machine
            restart_command = "; ssh %s 'docker restart cbioportal'" % self.server_name
        return [docker_command, python_command, restart_command]


class GitVersionTask(BaseTask):
    commit_hexsha = luigi.Parameter(description='commit to come back to')
    succeeded_once = False

    def run(self):
        repo.head.reference = repo.commit(self.commit_hexsha)

    def calc_done_signal(self):
        return calc_done_signal_content(get_checksum_pairs_set(config.input_data_dir))

    def complete(self):
        if self.succeeded_once:
            return True
        else:
            self.succeeded_once = repo.head.commit.hexsha == self.commit_hexsha
        return self.succeeded_once


class LoadDataFromNewFilesTask(luigi.WrapperTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tasks_dependency_tree = list(self._build_task_dependency_tree())

    @classmethod
    def _build_task_dependency_tree(self):
        logger.debug('Building the complete workflow ...')
        update_data_files = UpdateDataFiles()
        update_data_files.required_tasks = []
        yield update_data_files
        commit_input_data = GitCommit(directory_to_add=config.input_data_dir,
                                      commit_message='Add new input data.')
        commit_input_data.required_tasks = [update_data_files]
        yield commit_input_data
        format_codebook = FormatCodeBooks()
        format_codebook.required_tasks = [commit_input_data]
        yield format_codebook
        merge_clinical_data = MergeClinicalData()
        merge_clinical_data.required_tasks = [format_codebook]
        yield merge_clinical_data

        transmart_data_transformation = TransmartDataTransformation()
        transmart_data_transformation.required_tasks = [merge_clinical_data]
        yield transmart_data_transformation
        commit_transmart_staging = GitCommit(directory_to_add=config.transmart_staging_dir,
                                             commit_message='Add transmart data.')
        commit_transmart_staging.required_tasks = [transmart_data_transformation]
        yield commit_transmart_staging

        load_transmart_study = TransmartDataLoader()
        load_transmart_study.required_tasks = [commit_transmart_staging]
        yield load_transmart_study

        transmart_api_task = TransmartApiTask()
        transmart_api_task.required_tasks = [load_transmart_study]
        yield transmart_api_task

        commit_transmart_load_logs = GitCommit(directory_to_add=config.transmart_load_logs_dir,
                                               commit_message='Add transmart loading log.')
        commit_transmart_load_logs.required_tasks = [transmart_api_task]
        yield commit_transmart_load_logs

        cbioportal_data_transformation = CbioportalDataTransformation()
        cbioportal_data_transformation.required_tasks = [merge_clinical_data]
        yield cbioportal_data_transformation
        cbioportal_data_validation = CbioportalDataValidation()
        cbioportal_data_validation.required_tasks = [cbioportal_data_transformation]
        yield cbioportal_data_validation
        commit_cbio_staging = GitCommit(directory_to_add=config.cbioportal_staging_dir,
                                        commit_message='Add cbioportal data.')
        commit_cbio_staging.required_tasks = [cbioportal_data_validation]
        yield commit_cbio_staging
        cbioportal_data_loading = CbioportalDataLoading()
        cbioportal_data_loading.required_tasks = [commit_cbio_staging]
        yield cbioportal_data_loading
        commit_cbio_load_logs = GitCommit(directory_to_add=config.cbioportal_load_logs_dir,
                                          commit_message='Add cbioportal loading log.')
        commit_cbio_load_logs.required_tasks = [cbioportal_data_loading]
        yield commit_cbio_load_logs

    def requires(self):
        return self.tasks_dependency_tree


class e2e_LoadDataFromNewFilesTaskTransmartOnly(luigi.WrapperTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tasks_dependency_tree = list(self._build_task_dependency_tree())

    @classmethod
    def _build_task_dependency_tree(self):
        logger.debug('Building the complete workflow ...')
        update_data_files = UpdateDataFiles()
        update_data_files.required_tasks = []
        yield update_data_files
        commit_input_data = GitCommit(directory_to_add=config.input_data_dir,
                                      commit_message='Add new input data.')
        commit_input_data.required_tasks = [update_data_files]
        yield commit_input_data
        format_codebook = FormatCodeBooks()
        format_codebook.required_tasks = [commit_input_data]
        yield format_codebook
        merge_clinical_data = MergeClinicalData()
        merge_clinical_data.required_tasks = [format_codebook]
        yield merge_clinical_data

        transmart_data_transformation = TransmartDataTransformation()
        transmart_data_transformation.required_tasks = [merge_clinical_data]
        yield transmart_data_transformation
        commit_transmart_staging = GitCommit(directory_to_add=config.transmart_staging_dir,
                                             commit_message='Add transmart data.')
        commit_transmart_staging.required_tasks = [transmart_data_transformation]
        yield commit_transmart_staging

        load_transmart_study = TransmartDataLoader()
        load_transmart_study.required_tasks = [commit_transmart_staging]
        yield load_transmart_study

        commit_transmart_load_logs = GitCommit(directory_to_add=config.transmart_load_logs_dir,
                                               commit_message='Add transmart loading log.')
        commit_transmart_load_logs.required_tasks = [load_transmart_study]
        yield commit_transmart_load_logs

    def requires(self):
        return self.tasks_dependency_tree


if __name__ == '__main__':
    luigi.run()
