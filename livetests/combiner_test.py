from livetests.livetest import LiveTestCase
from livetests.jobs.mr_double_word_freq_count import MRWordFreqCount

class CombinerLiveTestCase(LiveTestCase):
    # this is required
    TEST_PATH = __file__

    BASIC_TESTS = True
    MR_JOB_CLASS = MRWordFreqCount
    INPUT_LINES = ['a b a\n']
    OUTPUT_LINES = ['"a"\t4', '"b"\t2']
