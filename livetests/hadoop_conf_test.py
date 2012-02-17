try:
    import json
except ImportError:
    import simplejson as json

from livetests.livetest import LiveTestCase
from livetests.jobs.mr_hadoop_conf import MRHadoopConfTest

class HadoopConfLiveTestCase(LiveTestCase):
    # this is required
    TEST_PATH = __file__

    INPUT_LINES = ['11.12.1.2',
                   '11.14.2.3',
                   '11.11.4.1',
                   '11.12.1.1',
                   '11.14.2.2',]

    def test_inline(self):
        output_lines = ['"keys_seen"\t%s' % json.dumps(sorted(self.INPUT_LINES))]
        output_lines += ['%s\tfalse' % json.dumps(l) for l in MRHadoopConfTest.JOBCONF.keys()]
        self.run_and_verify(MRHadoopConfTest, ['--runner', 'inline'],
                            input_lines=self.INPUT_LINES,
                            output_lines=output_lines)
