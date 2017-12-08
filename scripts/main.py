import luigi

class StartAll(luigi.WrapperTask):
    """
    Wrapper task whose purpose it is to check whether any tasks need be rerun
    to perform an update and then do it.
    """

    def requires(self):

        # yield Something()


if __name__ == '__main__':
    luigi.run()



