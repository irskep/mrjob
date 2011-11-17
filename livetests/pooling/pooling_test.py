# Copyright 2009-2011 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import sys
import time

from testify import assert_equal
from testify import setup
from testify import teardown

from livetests.livetest import LiveTestCase
from mrjob.emr import EMRJobRunner

class PoolingLiveTestCase(LiveTestCase):
    # this is required
    TEST_PATH = __file__

    # we only want the mrjob.conf, we don't want to use testconf.yaml
    GENERATE_FROM_CONFIG = False

    @setup
    def init_job_flow_list(self):
        self.job_flow_ids_to_terminate = []

    @teardown
    def kill_job_flows(self):
        runner = self._make_runner()
        for jf_id in self.job_flow_ids_to_terminate:
            runner.make_emr_conn().terminate_jobflow(jf_id)

    def _config_path(self):
        livetest_conf_path = os.path.join(self.livetest_base_dir, 'mrjob.conf')
        test_conf_path = os.path.join(self.test_base_dir, 'mrjob.conf')

        if os.path.exists(test_conf_path):
            return test_conf_path
        else:
            return livetest_conf_path

    def _make_runner(self, **kwargs):
        runner = EMRJobRunner(conf_path=self._config_path(),
                              **kwargs)
        return runner

    def _make_pooled_job_flow(self, pool_name=None, **kwargs):
        runner = EMRJobRunner(conf_path=self._config_path(),
                              pool_emr_job_flows=True,
                              emr_job_flow_pool_name=pool_name,
                              **kwargs)
        jf_id = runner.make_persistent_job_flow()
        self.job_flow_ids_to_terminate.append(jf_id)
        return runner

    def _wait_for_job_flow_to_wait(self, runner):
        jf = runner._describe_jobflow()
        while jf.state != 'WAITING':
            if jf.state in ('TERMINATED', 'COMPLETED', 'SHUTTING_DOWN',
                            'FAILED'):
                reason = getattr(jf, 'laststatechangereason', '')
                raise Exception('%s: %s' % (jf.state, reason))
            time.sleep(runner._opts['check_emr_status_every'])
            sys.stderr.write(jf.state[0])
            sys.stderr.flush()
            jf = runner._describe_jobflow()

    def test_none_exists_no_name(self):
        runner = self._make_runner(pool_emr_job_flows=True)
        assert_equal(runner.usable_job_flows(), [])

    def test_none_exists_named(self):
        runner = self._make_runner(pool_emr_job_flows=True,
                                   emr_job_flow_pool_name='test_pool_name')
        assert_equal(runner.usable_job_flows(), [])

    def test_one_exists_no_name(self):
        pool_runner = self._make_pooled_job_flow()
        self._wait_for_job_flow_to_wait(pool_runner)
        runner = self._make_runner(pool_emr_job_flows=True)
        usable_job_flows = runner.usable_job_flows()
        assert_equal(len(usable_job_flows), 1)
        assert_equal(pool_runner._emr_job_flow_id, usable_job_flows[0])

    def test_one_exists_named(self):
        pool_runner = self._make_pooled_job_flow(pool_name='test_pool_name')
        self._wait_for_job_flow_to_wait(pool_runner)
        runner = self._make_runner(pool_emr_job_flows=True,
                                   emr_job_flow_pool_name='test_pool_name')
        usable_job_flows = runner.usable_job_flows()
        assert_equal(len(usable_job_flows), 1)
        assert_equal(pool_runner._emr_job_flow_id, usable_job_flows[0])

    def test_simultaneous_none_exist(self):
        runner = self._make_runner(pool_emr_job_flows=True,
                                   emr_job_flow_pool_name='test_pool_name')

    def test_simultaneous_one_exists(self):
        pass

    def test_simultaneous_two_exist(self):
        pass

    def test_one_exists_but_is_busy(self):
        pass

    def test_dont_join_worse(self):
        pass

    def test_do_join_better(self):
        pass

    def test_dont_join_wrong_name(self):
        pass
