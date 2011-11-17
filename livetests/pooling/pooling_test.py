import os

from testify import setup

from livetests.livetest import LiveTestCase

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
            config_path = conf_info.get('path', test_conf_path)
        else:
            config_path = conf_info.get('path', livetest_conf_path)

    def _make_runner(self, **kwargs):
        runner = EMRJobRunner(conf_path=self._config_path(),
                              **kwargs)
        return runner

    def _make_pooled_job_flow(self, pool_name=None, **kwargs)
        runner = EMRJobRunner(conf_path=self._config_path(),
                              pool_emr_job_flows=True,
                              emr_job_flow_pool_Name=pool_name,
                              **kwargs)
        return runner.make_persistent_job_flow()

    def test_none_exists_no_name(self):
        pass

    def test_none_exists_named(self):
        pass

    def test_one_exists_no_name(self):
        pass

    def test_one_exists_named(self):
        pass

    def test_simultaneous_none_exist(self):
        pass

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
