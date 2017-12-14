import luigi


class TransmartDataLoader(luigi.Task):
    """
    Task to load data to tranSMART
    """
    def run(self):
        pass
    
    def complete(self):
        return True

class CbioportalDataLoader(luigi.Task):
    """
    Task to load data to cBioPortal
    """
    def run(self):
        pass
    
    def complete(self):
        return True

class DataLoader(luigi.WrapperTask):
    """
    Wrapper task whose purpose it is to check whether any tasks need be rerun
    to perform an update and then do it.
    """
    
    def requires(self):
        yield TransmartDataLoader()
        yield CbioportalDataLoader()

if __name__ == '__main__':
    luigi.run()

