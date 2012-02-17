from livetests.livetest import LiveTestCase
from livetests.jobs.mr_word_freq_count import MRWordFreqCount


class BasicLiveTestCase(LiveTestCase):
    # this is required
    TEST_PATH = __file__

    BASIC_TESTS = True
    MR_JOB_CLASS = MRWordFreqCount
    INPUT_LINES = ['a b a\n']
    OUTPUT_LINES = ['"a"\t2', '"b"\t1']
