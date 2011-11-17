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
instead of the mock versions.

A basic live test is specified by a config file with the module path of a job,
the path to the config file (to be expressed as a command line arg, filesystem
path, or environment variable), and the text of the expected output.

Example::

    runners: ['emr', 'hadoop', 'inline', 'local']
    python_bins:            # default: [['python2.6']]
        - ['python2.5']
        - ['python2.6']
        - ['python2.7']
    jobs:
        - job: mrjob.examples.mr_word_freq_count
          args: []                              # this is the default
          input: livetests/basic/input.txt      # this is the default, can
                                                # also be a list of strings
          output: livetests/basic/output.txt    # this is the default
          config:
              method: command_line              # or env_var or null
              path: livetests/basic/mrjob.conf  # this is the default
                # if no config specified, this is the default behavior
                # if method is null, use normal precedence
"""

from __future__ import with_statement

import logging
import os
from subprocess import Popen
from subprocess import PIPE
import sys
import tempfile
import textwrap

from testify import TestCase
from testify import assert_equal
from testify import setup
from testify import teardown

import yaml


logging.basicConfig()
log = logging.getLogger('livetests')
log.addHandler(logging.StreamHandler(sys.stderr))


class LiveTestCase(TestCase):

#    @setup
#    def make_tmp_dir(self):
#        self.tmp_dir = tempfile.mkdtemp(prefix='mrjob-livetests')
#
#    @teardown
#    def rm_tmp_dir(self):
#        shutil.rmtree(self.tmp_dir)

    TEST_PATH = __file__

    @setup
    def load(self):
        # don't run as abstract
        if self.TEST_PATH == __file__:
            return
        base_dir = os.path.split(os.path.abspath(self.TEST_PATH))[0]
        with open(os.path.join(base_dir, 'testconf.yaml'), 'r') as f:
            self.config = yaml.load(f)

    def conf_info(self, job_data):
        conf_info = job_data.get('config', {})
        conf_info['method'] = conf_info.get('method', 'command_line')
        conf_info['path'] = conf_info.get(
            'path', os.path.join(self.TEST_PATH, 'mrjob.conf'))
        return conf_info

    def test_jobs(self):
        # don't run as abstract
        if self.TEST_PATH == __file__:
            return

        for python_bin in self.config.get('python_bins', [['python2.6']]):
            log.info('==== Running jobs for python_bin: %s ====' % python_bin)

            for job in self.config['jobs']:

                given_input = job.get('input', None)
                if given_input is None:
                    given_input = os.path.join(self.TEST_PATH, 'input.txt')
                if isinstance(given_input, basestring):
                    input_files = [given_input]
                else:
                    input_files = given_input

                input_files = [os.path.abspath(p) for p in input_files]

                for runner in self.config['runners']:
                    log.info('  == Running %s in runner: %s ==' % (job['job'], runner))
                    # todo: co)fig.method
                    conf_info = self.conf_info(job)
                    if conf_info['method'] is None:
                        config_args = []
                    else:
                        config_args = ['--conf-path',
                                       os.path.abspath(conf_info['path'])]
                    args = config_args + self.config.get('args', [])
                    call_args = (python_bin + ['-m', job['job']] +
                                 ['-r', runner] +
                                 args + input_files)
                    log.info('  ' + str(call_args))
                    p = Popen(
                        call_args,
                        stdout=PIPE,
                        stderr=PIPE,
                    )

                    while True:
                        line = p.stderr.readline()
                        if not line:
                            break
                        log.info(textwrap.fill(line, width=80,
                                               initial_indent='    '))
                    log.info('')

                    with open(job.get('output',
                                      os.path.join(self.TEST_PATH,
                                                   'output.txt')),
                              'r') as f:
                        a_lines = sorted(f.read().splitlines())
                        b_lines = sorted(p.communicate()[0].splitlines())
                        assert_equal(a_lines, b_lines)
