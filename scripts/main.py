import luigi


class CheckForFilesComplete(luigi.Task):
    """
    Task to check whether file transfers are complete
    """
    
    def run(self):
        pass
    
    def complete():
        return False


class CheckForNewFiles(luigi.Task):
    """
    Task to check whether new files are available
    """
    
    def requires(self):
        return CheckForFilesComplete()
        
    def run(self):
        pass
    
    def complete():
        return False


class GitAddRawFiles(luigi.Task):
    """
    Task to add raw data files to git
    """
    
    def requires(self):
        return CheckForNewFiles()
    
    def run(self):
        pass
    
    def complete(self):
        return False


class MergeClinicalData(luigi.Task):
    """
    Task to merge all clinical data files
    """
    
    def requires(self):
        return GitAddRawFiles()
    
    def run(self):
        pass
    
    def complete(self):
        return False
    

class TransmartDataTransformation(luigi.Task):
    """
    Task to transform data files for tranSMART
    """
    
    def requires(self):
        return MergeClinicalData()
    
    def run(self):
        pass
    
    def complete(self):
        return False


class CbioportalDataTransformation(luigi.Task):
    """
    Task to transform data files for cBioPortal
    """
    
    def requires(self):
        return MergeClinicalData()
    
    def run(self):
        pass
    
    def complete(self):
        return False


class GitAddStagingFilesAndCommit(luigi.Task):
    """
    Task to add transformed files to Git and commit
    """
    
    def requires(self):
        yield TransmartDataTransformation()
        yield CbioportalDataTransformation()

    def run(self):
        pass
    
    def complete(self):
        return False
    

class TransmartDataLoader(luigi.Task):
    """
    Task to load data to tranSMART
    """
    
    def requires(self):
        return GitAddStagingFilesAndCommit()
    
    def run(self):
        pass
    
    def complete(self):
        return False

class CbioportalDataLoader(luigi.Task):
    """
    Task to load data to cBioPortal
    """
    
    def requires(self):
        return GitAddStagingFilesAndCommit()
    
    def run(self):
        pass
    
    def complete(self):
        return False


class GitAmendLoadResults(luigi.Task):
    """
    Task to amend git commit results with load status
    """
    
    def requires(self):
        yield TransmartDataLoader()
        yield CbioportalDataLoader()
        
    def complete(self):
        return False

class DataLoader(luigi.WrapperTask):
    """
    Wrapper task whose purpose it is to check whether any tasks need be rerun
    to perform an update and then do it.
    """
    
    def requires(self):
        return GitAmendLoadResults()

if __name__ == '__main__':
    luigi.run()

