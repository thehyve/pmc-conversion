import logging
import os

import luigi
import time
from .git_commons import get_git_repo
from .sync import sync_dirs, is_dirs_in_sync, get_checksum_pairs_set
from .luigi_commons import BaseTask, ExternalProgramTask
from .codebook_formatting import codebook_formatting
from pathlib import Path

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
os.makedirs(os.path.join(config.intermediate_file_dir, 'cbioportal_staging_files'), exist_ok=True)
os.makedirs(os.path.join(config.intermediate_file_dir, 'cbioportal_report_files'), exist_ok=True)

def calc_done_signal_content(file_checksum_pairs):
    srt_file_checksum_pairs = sorted(file_checksum_pairs, key=lambda file_checksum_pair: file_checksum_pair.data_file)
    return '\n'.join([file + ' ' + checksum for file, checksum in srt_file_checksum_pairs])


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


class GitAddRawFiles(BaseTask):
    """
    Task to add raw data files to git
    """

    def run(self):
        repo.index.add([config.input_data_dir])


class FormatCodeBooks(BaseTask):
    cm_map_file = luigi.Parameter(description='Codebook mapping file', significant=False)

    def run(self):
        cm_map = os.path.join(config.config_json_dir, self.cm_map_file)

        for path, dir_, filenames in os.walk(config.input_data_dir):
            codebooks = [file for file in filenames if 'codebook' in file]
            for codebook in codebooks:
                codebook_file = os.path.join(path, codebook)
                codebook_formatting(codebook_file, cm_map, config.intermediate_file_dir)


class MergeClinicalData(ExternalProgramTask):

    wd = os.path.join(os.getcwd(), 'scripts')

    csr_transformation = luigi.Parameter(description='CSR transformation script name', significant=False)
    data_model = luigi.Parameter(description='JSON file with the columns per entity', significant=False)
    file_list = luigi.Parameter(description='Flat text file with an ordered list of expected files', significant=False)
    file_headers = luigi.Parameter(description='JSON file with a list of columns per data file', significant=False,
                                   default=None)
    columns_to_csr = luigi.Parameter(
        description='Data file columns mapped to expected fields in the central subject registry', significant=False)

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

    def program_args(self):

        clinical_input_file = os.path.join(config.intermediate_file_dir, config.csr_data_file)
        ngs_dir = os.path.join(config.drop_dir, 'NGS')
        output_dir = os.path.join(config.intermediate_file_dir, 'cbioportal_staging_files')

        return ['python3 cbioportal_transformation/pmc_cbio_wrapper.py',
                '-c', clinical_input_file,
                '-n', ngs_dir,
                '-o', output_dir]


class GitAddStagingFilesAndCommit(BaseTask):
    """
    Task to add transformed files to Git and commit
    """

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

    def program_args(self):
        return ['java', '-jar', '{!r}'.format(config.transmart_copy_jar), '--delete', '{!r}'.format(config.study_id)]


class LoadTransmartStudy(TransmartDataLoader):
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

    # Set specific docker image
    docker_image = luigi.Parameter(description='cBioPortal docker image', significant=False)

    # Success codes for validation
    success_codes = [0, 3]

    def program_args(self):
        # Directory and file names for validation
        input_dir = os.path.join(config.intermediate_file_dir, 'cbioportal_staging_files')
        report_dir = os.path.join(config.intermediate_file_dir, 'cbioportal_report_files')
        db_info_dir = os.path.join(config.config_json_dir, 'cbioportal_db_info')
        report_name = 'report_pmc_test_%s.html' % time.strftime("%Y%m%d-%H%M%S")

        # Build the command for validation
        docker_command = 'docker run --rm -v %s:/study/ -v /etc/hosts:/etc/hosts ' \
                         '-v %s:/cbioportal_db_info/ -v %s:/html_reports/ %s' \
                         % (input_dir, db_info_dir, report_dir, self.docker_image)

        python_command = 'python /cbioportal/core/src/main/scripts/importer/validateData.py -s /study/ ' \
                         '-P /cbioportal/src/main/resources/portal.properties ' \
                         '-p /cbioportal_db_info -html /html_reports/%s -v' \
                         % report_name
        return [docker_command, python_command]

    def run(self):
        pass


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

    # Variables
    docker_image = luigi.Parameter(description='cBioPortal docker image', significant=False)
    server_name = luigi.Parameter(description='Server on which pipeline is running. If running docker locally, leave '
                                              'empty. PMC servers: pmc-cbioportal-test | '
                                              'pmc-cbioportal-acc | pmc-cbioportal-prod', significant=False)

    def program_args(self):

        # Directory and file names for validation
        input_dir = os.path.join(config.intermediate_file_dir, 'cbioportal_staging_files')

        # Build the command for importer only
        docker_command = 'docker run --rm -v %s:/study/ -v /etc/hosts:/etc/hosts %s' \
                         % (input_dir, self.docker_image)
        python_command = 'python /cbioportal/core/src/main/scripts/importer/cbioportalImporter.py -s /study/'

        # Check if cBioPortal is running locally or on other server
        if self.server_name == "":
            restart_command = "; docker restart cbioportal"
        else:
            restart_command = "; ssh %s 'docker restart cbioportal'" % self.server_name
        return [docker_command, python_command, restart_command]

    def run(self):
        pass


class GitCommitLoadResults(BaseTask):
    """
    Task to amend git commit results with load status
    """

    def run(self):
        repo.index.add([config.load_logs_dir])
        repo.index.commit('Add load results.')


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


def load_clinical_data_workflow(required_tasks):
    delete_transmart_study_if_exists = DeleteTransmartStudyIfExists()
    delete_transmart_study_if_exists.required_tasks = [required_tasks]
    yield delete_transmart_study_if_exists
    load_transmart_study = LoadTransmartStudy()
    load_transmart_study.required_tasks = [delete_transmart_study_if_exists]
    yield load_transmart_study
    cbioportal_data_validation = CbioportalDataValidation()
    cbioportal_data_validation.required_tasks = [required_tasks]
    yield cbioportal_data_validation
    cbioportal_data_loading = CbioportalDataLoading()
    cbioportal_data_loading.required_tasks = [cbioportal_data_validation]
    yield cbioportal_data_loading
    git_commit_load_results = GitCommitLoadResults()
    git_commit_load_results.required_tasks = [cbioportal_data_loading, load_transmart_study]
    yield git_commit_load_results


class LoadDataFromNewFilesTask(luigi.WrapperTask):
    """
    Wrapper task whose purpose it is to check whether any tasks need be rerun
    to perform an update and then do it.
    """

    @property
    def tasks_dependency_tree(self):
        logger.debug('Building workflow ...')
        update_data_files = UpdateDataFiles()
        update_data_files.required_tasks = []
        yield update_data_files
        git_add_raw_files = GitAddRawFiles()
        git_add_raw_files.required_tasks = [update_data_files]
        yield git_add_raw_files
        format_codebook = FormatCodeBooks()
        format_codebook.required_tasks = [git_add_raw_files]
        yield format_codebook
        merge_clinical_data = MergeClinicalData()
        merge_clinical_data.required_tasks = [format_codebook]
        yield merge_clinical_data
        transmart_data_transformation = TransmartDataTransformation()
        transmart_data_transformation.required_tasks = [merge_clinical_data]
        yield transmart_data_transformation
        cbioportal_data_transformation = CbioportalDataTransformation()
        cbioportal_data_transformation.required_tasks = [merge_clinical_data]
        yield cbioportal_data_transformation
        git_add_staging_files_and_commit = GitAddStagingFilesAndCommit()
        git_add_staging_files_and_commit.required_tasks = [cbioportal_data_transformation, transmart_data_transformation]
        yield git_add_staging_files_and_commit
        for task in load_clinical_data_workflow(git_add_staging_files_and_commit):
            yield task

    def requires(self):
        return self.tasks_dependency_tree


class LoadDataFromHistoryTask(luigi.WrapperTask):

    @property
    def tasks_dependency_tree(self):
        git_version = GitVersionTask()
        yield git_version
        for task in load_clinical_data_workflow(git_version):
            yield task

    def requires(self):
        return self.tasks_dependency_tree

    @classmethod
    def remove_all_signal_files(cls):
        for p in Path('.').glob('.done-*'):
            logger.debug('Remove done signal file: {}'.format(p))
            p.unlink()

    def on_success(self):
        # if we did not delete signal files loading new files would fail on data loading step.
        self.remove_all_signal_files()

if __name__ == '__main__':
    luigi.run()
