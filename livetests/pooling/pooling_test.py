import os

from testify import setup

from livetests.livetest import LiveTestCase

class PoolingLiveTestCase(LiveTestCase):
    # this is required
    TEST_PATH = __file__

    # we only want the mrjob.conf, we don't want to use testconf.yaml
    GENERATE_FROM_CONFIG = False

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
