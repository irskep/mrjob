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
from testify import assert_not_equal
from testify import setup
from testify import teardown

from livetests.jobs.mr_word_freq_count import MRWordFreqCount
from livetests.livetest import LiveTestCase
from mrjob.emr import EMRJobRunner


class PoolingLiveTestCase(LiveTestCase):

    @setup
    def init_job_flow_list(self):
        self.job_flow_ids_to_terminate = []

    @teardown
    def kill_job_flows(self):
        runner = self._make_runner()
        for jf_id in self.job_flow_ids_to_terminate:
            runner.make_emr_conn().terminate_jobflow(jf_id)

    def _make_runner(self, **kwargs):
        runner = EMRJobRunner(**kwargs)
        return runner

    def make_pooled_job_flow(self, pool_name=None, **kwargs):
        runner = EMRJobRunner(pool_emr_job_flows=True,
                              emr_job_flow_pool_name=pool_name,
                              **kwargs)
        jf_id = runner.make_persistent_job_flow()
        self.job_flow_ids_to_terminate.append(jf_id)
        return runner, jf_id

    def assertJoins(self, job_flow_id, job_args, job_class=MRWordFreqCount):
        with job_class(job_args).make_runner() as runner:
            tmp_runner = EMRJobRunner(emr_job_flow_id=job_flow_id)
            self._wait_for_job_flow_to_wait(tmp_runner)
            assert_equal(self._usable_job_flows_for_runner(runner),
                         [job_flow_id])

    def assertDoesNotJoin(self, job_flow_id, job_args, job_class=MRWordFreqCount):
        with job_class(job_args).make_runner() as runner:
            tmp_runner = EMRJobRunner(emr_job_flow_id=job_flow_id)
            self._wait_for_job_flow_to_wait(tmp_runner)
            assert_not_equal(self._usable_job_flows_for_runner(runner),
                             [job_flow_id])

    def _wait_for_job_flow_to_wait(self, runner):
        jf = runner._describe_jobflow()
        while jf.state != 'WAITING':
            if jf.state in ('TERMINATED', 'COMPLETED', 'SHUTTING_DOWN',
                            'FAILED'):
                reason = getattr(jf, 'laststatechangereason', '')
                raise Exception('%s: %s' % (jf.state, reason))
            time.sleep(runner._opts['check_emr_status_every'])
            sys.stderr.write(jf.state[0:2])
            sys.stderr.write('.')
            sys.stderr.flush()
            jf = runner._describe_jobflow()

    def _usable_job_flows_for_runner(self, runner):
        return [jf.jobflowid for jf in runner.usable_job_flows()]

    def test_none_exists_no_name(self):
        runner = self._make_runner(pool_emr_job_flows=True)
        assert_equal(self._usable_job_flows_for_runner(runner), [])

    def test_none_exists_named(self):
        runner = self._make_runner(pool_emr_job_flows=True,
                                   emr_job_flow_pool_name='test_pool_name')
        assert_equal(self._usable_job_flows_for_runner(runner), [])

    def test_one_exists_no_name(self):
        pool_runner, jf_id = self.make_pooled_job_flow()
        self._wait_for_job_flow_to_wait(pool_runner)
        runner = self._make_runner(pool_emr_job_flows=True)
        assert_equal(self._usable_job_flows_for_runner(runner), [jf_id])

    def test_one_exists_named(self):
        pool_runner, jf_id = self.make_pooled_job_flow(pool_name='test_pool_name')
        self._wait_for_job_flow_to_wait(pool_runner)
        runner = self._make_runner(pool_emr_job_flows=True,
                                   emr_job_flow_pool_name='test_pool_name')
        assert_equal(self._usable_job_flows_for_runner(runner), [jf_id])

    def test_dont_join_wrong_name(self):
        pool_runner, jf_id = self.make_pooled_job_flow(pool_name='test_pool_name_NOT')
        self._wait_for_job_flow_to_wait(pool_runner)
        runner = self._make_runner(pool_emr_job_flows=True,
                                   emr_job_flow_pool_name='test_pool_name')
        assert_equal(self._usable_job_flows_for_runner(runner), [])

    def test_one_exists_but_is_not_waiting(self):
        pool_runner, jf_id = self.make_pooled_job_flow()
        runner = self._make_runner(pool_emr_job_flows=True)
        assert_equal(self._usable_job_flows_for_runner(runner), [])

    def test_dont_join_worse(self):
        pool_runner, jf_id = self.make_pooled_job_flow(pool_name='test_pool_name')
        self._wait_for_job_flow_to_wait(pool_runner)
        runner = self._make_runner(pool_emr_job_flows=True,
                                   emr_job_flow_pool_name='test_pool_name',
                                   num_ec2_instances=2)
        assert_equal(self._usable_job_flows_for_runner(runner), [])

    def test_do_join_better(self):
        pool_runner, jf_id = self.make_pooled_job_flow(
            pool_name='test_pool_name', num_ec2_instances=2)
        self._wait_for_job_flow_to_wait(pool_runner)
        runner = self._make_runner(pool_emr_job_flows=True,
                                   emr_job_flow_pool_name='test_pool_name',
                                   num_ec2_instances=1)
        assert_equal(self._usable_job_flows_for_runner(runner), [jf_id])

    def test_simultaneous_none_exist(self):
        runner1 = self._make_runner(pool_emr_job_flows=True,
                                    emr_job_flow_pool_name='test_pool_name')
        runner2 = self._make_runner(pool_emr_job_flows=True,
                                    emr_job_flow_pool_name='test_pool_name')

        jf_id_1 = runner1.find_job_flow()
        jf_id_2 = runner2.find_job_flow()

        assert_equal(jf_id_1, None)
        assert_equal(jf_id_2, None)

    def test_simultaneous_one_exists(self):
        runner1 = self._make_runner(pool_emr_job_flows=True,
                                    emr_job_flow_pool_name='test_pool_name')
        runner2 = self._make_runner(pool_emr_job_flows=True,
                                    emr_job_flow_pool_name='test_pool_name')

        pool_runner_1, jf_id_canonical = self.make_pooled_job_flow(
            pool_name='test_pool_name')

        self._wait_for_job_flow_to_wait(pool_runner_1)

        jf_id_1 = runner1.find_job_flow()
        jf_id_2 = runner2.find_job_flow()

        assert_equal(jf_id_1.jobflowid, jf_id_canonical)
        assert_equal(jf_id_2, None)

    def test_simultaneous_two_exist(self):
        runner1 = self._make_runner(pool_emr_job_flows=True,
                                    emr_job_flow_pool_name='test_pool_name')
        runner2 = self._make_runner(pool_emr_job_flows=True,
                                    emr_job_flow_pool_name='test_pool_name')

        pool_runner_1, jf_id_canonical_1 = self.make_pooled_job_flow(
            pool_name='test_pool_name')

        pool_runner_2, jf_id_canonical_2 = self.make_pooled_job_flow(
            pool_name='test_pool_name')

        # find_job_flow() acquires locks, whereas usable_job_flows() does not
        jf_id_1 = runner1.find_job_flow()
        jf_id_2 = runner2.find_job_flow()

        if jf_id_1 == jf_id_canonical_1:
            assert_equal(jf_id_1, jf_id_canonical_1) # lol
            assert_equal(jf_id_2, jf_id_canonical_2)
        else:
            assert_equal(jf_id_1, jf_id_canonical_2)
            assert_equal(jf_id_2, jf_id_canonical_1)

    def test_can_join_job_flow_with_same_bid_price(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_master_instance_bid_price='0.25')

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-master-instance-bid-price', '0.25'])

    def test_can_join_job_flow_with_higher_bid_price(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_master_instance_bid_price='25.00')

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-master-instance-bid-price', '0.25'])

    def test_cant_join_job_flow_with_lower_bid_price(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_master_instance_bid_price='0.25',
            num_ec2_instances=100)

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-master-instance-bid-price', '25.00'])

    def test_on_demand_satisfies_any_bid_price(self):
        _, job_flow_id = self.make_pooled_job_flow()

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-master-instance-bid-price', '25.00'])

    def test_no_bid_price_satisfies_on_demand(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_master_instance_bid_price='25.00')

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows'])

    def test_core_and_task_instance_types(self):
        # a tricky test that mixes and matches different criteria
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_core_instance_bid_price='0.25',
            ec2_task_instance_bid_price='25.00',
            ec2_task_instance_type='c1.xlarge',
            num_ec2_core_instances=2,
            num_ec2_task_instances=3)

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--num-ec2-core-instances', '2',
            '--num-ec2-task-instances', '10',  # more instances, but smaller
            '--ec2-core-instance-bid-price', '0.10',
            '--ec2-master-instance-bid-price', '77.77',
            '--ec2-task-instance-bid-price', '22.00'])
