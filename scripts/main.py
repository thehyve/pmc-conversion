import os

import luigi

from checksum import read_sha1_file

from sync import get_checksum_pairs_set, sync_dirs, same_data


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
            return read_sha1_file(self.input_signal_file[0])
        else:
            return read_sha1_file(self.input_signal_file)

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

    drop_dir = luigi.Parameter(description='Directory to copy data files from.')
    staging_dir = luigi.Parameter(description='Directory to copy data files to.')

    done_signal_filename = '.done-CheckForNewFiles'

    def run(self):
        sync_dirs(self.drop_dir, self.staging_dir)

    def complete(self):
        return same_data(self.drop_dir, self.staging_dir)

    def calc_done_signal(self):
        """
        :return: file with list of files and their checksums
        """
        file_checksum_pairs = sorted(get_checksum_pairs_set(self.staging_dir),
                                     key=lambda file_checksum_pair: file_checksum_pair[0])
        return '\n'.join([file + ' ' + checksum for file, checksum in file_checksum_pairs])


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
        pass


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
        pass


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


class GitAmendLoadResults(BaseTask):
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


class DataLoader(luigi.WrapperTask):
    """
    Wrapper task whose purpose it is to check whether any tasks need be rerun
    to perform an update and then do it.
    """
    
    def requires(self):
        return GitAmendLoadResults()


if __name__ == '__main__':
    luigi.run()

