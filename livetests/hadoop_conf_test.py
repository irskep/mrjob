try:
    import json
except ImportError:
    import simplejson as json

from livetests.livetest import LiveTestCase
from livetests.jobs.mr_hadoop_conf import MRHadoopConfTest

class HadoopConfLiveTestCase(LiveTestCase):
    # this is required
    TEST_PATH = __file__

    INPUT_KEYS = ['11.12.1.2',
                  '11.14.2.3',
                  '11.11.4.1',
                  '11.12.1.1',
                  '11.14.2.2',]
    #INPUT_LINES = ['%s\tx' % k for k in INPUT_KEYS]
    INPUT_LINES = INPUT_KEYS

    SKIP_EMR = False

    def test_inline(self):
        # doesn't use real partitioner
        output_lines = ['"keys_seen"\t%s' % json.dumps(sorted(self.INPUT_KEYS))]
        # doesn't set env vars
        output_lines += ['%s\tfalse' % json.dumps(l) for l in MRHadoopConfTest.JOBCONF.keys()]
        self.run_and_verify(MRHadoopConfTest, ['--runner', 'inline'],
                            input_lines=self.INPUT_LINES,
                            output_lines=output_lines)

    def test_local(self):
        # doesn't use real partitioner
        # each reducer gets one
        output_lines = ['"keys_seen"\t%s' % json.dumps([k]) for k in sorted(self.INPUT_KEYS)]
        # sets env vars
        # there are 5 reducers, so 5 copies of each value
        all_keys_ever = [
            'map.output.key.field.separator',
            'mapred.reduce.tasks',
            'num.key.fields.for.partition',
            'stream.map.output.field.separator',
            'stream.num.map.output.key.fields',
        ] * 5
        output_lines += ['%s\ttrue' % json.dumps(l) for l in all_keys_ever]
        self.run_and_verify(MRHadoopConfTest, ['--runner', 'local'],
                            input_lines=self.INPUT_LINES,
                            output_lines=output_lines)

    def test_emr(self):
        # doesn't use real partitioner
        output_lines = ['"keys_seen"\t%s' % json.dumps(sorted(self.INPUT_KEYS))]
        # sets env vars
        output_lines += ['%s\ttrue' % json.dumps(l) for l in MRHadoopConfTest.JOBCONF.keys()]
        self.run_and_verify(MRHadoopConfTest, ['--runner', 'emr', '--strict-protocols'],
                            input_lines=self.INPUT_LINES,
                            output_lines=output_lines)
