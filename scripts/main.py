import logging
import os

import luigi
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

    cm_map = '/Users/wibopipping/Projects/PMC/pmc-conversion/config/codebook_mapping.json'
    output_dir = '/Users/wibopipping/Projects/PMC/csr_luigi_pipeline/intermediate'

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

    def program_args(self):
        return [self.python_version, self.csr_transformation, self.csr_config, '--config_dir', self.config_dir]


class TransmartDataTransformation(ExternalProgramTask):
    wd = luigi.Parameter(description='Working directory with the tranSMART transformation script', significant=False)
    tm_transformation = luigi.Parameter(description='tranSMART data transformation script name', significant=False)
    tm_config = luigi.Parameter(description='tranSMART data transformation config file', significant=False)
    python_version = luigi.Parameter(description='Python command to use to execute', significant=False)
    config_dir = luigi.Parameter(description='tranSMART transformation config dir', significant=False)

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
        return ['java', '-jar', '{!r}'.format(config.transmart_copy_jar), '-d', '{!r}'.format(config.skinny_dir)]


class CbioportalDataLoader(ExternalProgramTask):
    """
    Task to load data to cBioPortal
    """

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
    cbioportal_data_loader = CbioportalDataLoader()
    cbioportal_data_loader.required_tasks = [required_tasks]
    yield cbioportal_data_loader
    git_commit_load_results = GitCommitLoadResults()
    git_commit_load_results.required_tasks = [cbioportal_data_loader, load_transmart_study]
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
        format_condebook = FormatCodeBooks()
        format_condebook.required_tasks = [git_add_raw_files]
        yield format_condebook
        merge_clinical_data = MergeClinicalData()
        merge_clinical_data.required_tasks = [format_condebook]
        yield merge_clinical_data
        transmart_data_transformation = TransmartDataTransformation()
        transmart_data_transformation.required_tasks = [merge_clinical_data]
        yield transmart_data_transformation
        cbioportal_data_transformation = CbioportalDataTransformation()
        cbioportal_data_transformation.required_tasks = [transmart_data_transformation]
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
