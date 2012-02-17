from livetests.livetest import LiveTestCase
from livetests.jobs.mr_hadoop_conf import MRHadoopConfTest

class HadoopConfLiveTestCase(LiveTestCase):
    # this is required
    TEST_PATH = __file__

    BASIC_TESTS = True
    MR_JOB_CLASS = MRHadoopConfTest
    INPUT_LINES = ['11.12.1.2',
                   '11.14.2.3',
                   '11.11.4.1',
                   '11.12.1.1',
                   '11.14.2.2',]
    OUTPUT_LINES = ['"a"\t4', '"b"\t2']
