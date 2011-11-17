import os

from testify import setup

from livetests.livetest import LiveTestCase

class BasicLiveTestCase(LiveTestCase):
    # this is required
    TEST_PATH = __file__
