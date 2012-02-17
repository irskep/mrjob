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

"""Live tests are run in a live environment, e.g. actual EMR and Hadoop
instead of the mock versions. Your regular mrjob.conf is used.
"""

from __future__ import with_statement

import logging
import os
from StringIO import StringIO
import sys

from testify import TestCase
from testify import assert_equal
from testify import setup
from testify import teardown


logging.basicConfig()
log = logging.getLogger('livetests')
#log.addHandler(logging.StreamHandler(sys.stderr))


class LiveTestCase(TestCase):

    # Path to the test file, e.g. /x/y/livetests/basic/basic_test.py
    # should be overridden by every subclass, just copy this line
    TEST_PATH = __file__

    SKIP_HADOOP = True  # if you don't have Hadoop installed locally
    SKIP_EMR = True  # if you don't feel like waiting for EMR

    # override these if you want to use simple_run
    BASIC_TESTS = False
    MR_JOB_CLASS = None
    INPUT_LINES = None
    OUTPUT_LINES = None

    def __init__(self, *args, **kwargs):
        self.test_base_dir = os.path.split(os.path.abspath(self.TEST_PATH))[0]
        self.livetest_base_dir = os.path.split(os.path.abspath(__file__))[0]

        super(LiveTestCase, self).__init__(*args, **kwargs)

    def run_and_verify(self, MRJobClass, args, input_lines, output_lines):
        if self.SKIP_HADOOP and 'hadoop' in args:
            return
        if self.SKIP_EMR and 'emr' in args:
            return
        mr_job = MRJobClass(args)
        mr_job.stdin = StringIO('\n'.join(input_lines))
        with mr_job.make_runner() as runner:
            runner.run()
            results = sorted(s.rstrip('\r\n') for s in runner.stream_output())
            assert_equal(results, output_lines)

    def simple_run(self, runner_name, version=None):
        args = ['--runner', runner_name]
        if version:
            args.extend(['--hadoop-version', version])
        self.run_and_verify(self.MR_JOB_CLASS, args=args,
                            input_lines=self.INPUT_LINES,
                            output_lines=self.OUTPUT_LINES)

    def test_inline(self):
        if not self.BASIC_TESTS:
            return
        self.simple_run('inline')

    def test_local(self):
        if not self.BASIC_TESTS:
            return
        self.simple_run('local')

    def test_emr(self):
        if not self.BASIC_TESTS:
            return
        self.simple_run('emr')

    def test_hadoop(self):
        if not self.BASIC_TESTS:
            return
        self.simple_run('hadoop')
