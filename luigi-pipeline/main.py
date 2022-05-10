import asyncio
import logging
import os
import shutil
import threading

import luigi
from csr2transmart import csr2transmart
from sources2csr.sources2csr import sources2csr

from scripts.git_commons import get_git_repo
from scripts.sync import sync_dirs, get_checksum_pairs_set
from scripts.transmart_api_calls import TransmartApiCalls
from .luigi_commons import BaseTask, ExternalProgramTask

logger = logging.getLogger('luigi')


class GlobalConfig(luigi.Config):
    drop_dir = luigi.Parameter(description='Directory files gets uploaded to.')

    data_repo_dir = luigi.Parameter(description='Path to the staging git repository.')

    load_logs_dir_name = luigi.Parameter(description='Path to the log files of the loading scripts.',
                                         default='load_logs')
    working_dir = luigi.Parameter(description='Path to the repo to store the intermediate data products.')

    transformation_config_dir = luigi.Parameter(description='Folder with mapping files in JSON format.')

    transmart_copy_jar = luigi.Parameter(description='Path to transmart copy jar.')
    study_id = luigi.Parameter(description="Id of the study to load.")
    top_node = luigi.Parameter(description='Topnode of the study to load.')

    PGHOST = luigi.Parameter(description="Configuration for transmart-copy.")
    PGPORT = luigi.Parameter(description="Configuration for transmart-copy.")
    PGDATABASE = luigi.Parameter(description="Configuration for transmart-copy.")
    PGUSER = luigi.Parameter(description="Configuration for transmart-copy.", significant=True)
    PGPASSWORD = luigi.Parameter(description="Configuration for transmart-copy.", significant=False)

    @property
    def input_data_dir(self):
        return os.path.join(self.data_repo_dir, 'input_data')

    @property
    def transmart_staging_dir(self):
        return os.path.join(self.data_repo_dir, 'staging', 'transmart')

    @property
    def load_logs_dir(self):
        return os.path.join(self.data_repo_dir, self.load_logs_dir_name)

    @property
    def transmart_load_logs_dir(self):
        return os.path.join(self.load_logs_dir, 'transmart')


config = GlobalConfig()
git_lock = threading.RLock()
repo = get_git_repo(config.data_repo_dir)

os.makedirs(config.input_data_dir, exist_ok=True)
os.makedirs(config.transmart_staging_dir, exist_ok=True)
os.makedirs(config.transmart_load_logs_dir, exist_ok=True)


def calc_done_signal_content(file_checksum_pairs):
    srt_file_checksum_pairs = sorted(file_checksum_pairs, key=lambda file_checksum_pair: file_checksum_pair.data_file)
    return '\n'.join([file + ' ' + checksum for file, checksum in srt_file_checksum_pairs])


class GitCommit(BaseTask):
    directory_to_add = luigi.Parameter(description='Directory content of which to commit.')
    commit_message = luigi.Parameter(description='Commit message.')

    def run(self):
        with git_lock:
            repo.index.add([self.directory_to_add])
            if repo.is_dirty():
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


class Sources2CsrTransformation(BaseTask):
    """
    Task to transform source files to CSR intermediate files
    """
    def run(self):
        if os.path.isdir(config.working_dir):
            shutil.rmtree(config.working_dir)
        sources2csr(config.input_data_dir, config.working_dir, config.transformation_config_dir)


class TransmartDataTransformation(BaseTask):
    """
    Task to transform data from the CSR intermediate files to transmart-copy format
    """
    def run(self):
        if os.path.isdir(config.transmart_staging_dir):
            shutil.rmtree(config.transmart_staging_dir)
        csr2transmart.csr2transmart(config.working_dir,
                                    config.transmart_staging_dir,
                                    config.transformation_config_dir,
                                    config.study_id,
                                    config.top_node)


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
        return ['java', '-jar', '{!r}'.format(config.transmart_copy_jar),
                '--update-concept-paths',
                '--directory', '{!r}'.format(config.transmart_staging_dir)]


class TransmartApiTask(BaseTask):
    keycloak_url = luigi.Parameter(description='URL of the keycloak instance, include the realm in the URL')
    client_id = luigi.Parameter(description='client_id of transmart client configured in keycloak')
    offline_token = luigi.Parameter(description='offline_token used as refresh token to receive an actual access token,'
                                                'configured in keycloak')
    transmart_url = luigi.Parameter(description='URL of the tranSMART instance', significant=False)
    gb_backend_url = luigi.Parameter(description='URL of the gb backend instance', significant=False)

    max_status_check_retrial = 240

    def run(self):
        reload_obj = TransmartApiCalls(keycloak_url=self.keycloak_url,
                                       transmart_url=self.transmart_url,
                                       gb_backend_url=self.gb_backend_url,
                                       client_id=self.client_id,
                                       offline_token=self.offline_token)

        logger.info('After data loading update; clearing and rebuilding caches, rebuilding subject sets')
        reload_obj.after_data_loading()

        logger.info('Waiting for the update to complete ...')
        asyncio.run(reload_obj.check_status(self.max_status_check_retrial))

        logger.info('Scanning for new subscriptions')
        reload_obj.scan_subscription_queries()


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

    def _build_task_dependency_tree(self):
        logger.debug('Building the complete workflow ...')
        update_data_files = UpdateDataFiles()
        update_data_files.required_tasks = []
        yield update_data_files
        commit_input_data = GitCommit(directory_to_add=config.input_data_dir,
                                      commit_message='Add new input data.')
        commit_input_data.required_tasks = [update_data_files]
        yield commit_input_data

        sources_to_csr_task = Sources2CsrTransformation()
        sources_to_csr_task.required_tasks = [update_data_files]
        yield sources_to_csr_task

        csr_to_transmart_task = TransmartDataTransformation()
        csr_to_transmart_task.required_tasks = [sources_to_csr_task]
        yield csr_to_transmart_task

        commit_transmart_staging = GitCommit(directory_to_add=config.transmart_staging_dir,
                                             commit_message='Add transmart data.')
        commit_transmart_staging.required_tasks = [csr_to_transmart_task]
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

    def requires(self):
        return self.tasks_dependency_tree


if __name__ == '__main__':
    luigi.run()
