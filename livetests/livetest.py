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

    runners: ['emr-0.18', 'hadoop-0.20', 'inline', 'local']
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
              method: command_line              # default. or env_var or null
              path: livetests/basic/mrjob.conf  # this is the default
                # if no config specified, this is the default behavior
                # if method is null, use normal precedence
"""

from __future__ import with_statement

from distutils.version import LooseVersion
import logging
import os
import shutil
from subprocess import check_call
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


from mrjob.emr import EMRJobRunner
from mrjob.hadoop import HadoopJobRunner
from mrjob.local import LocalMRJobRunner
from mrjob.inline import InlineMRJobRunner


logging.basicConfig()
log = logging.getLogger('livetests')
log.addHandler(logging.StreamHandler(sys.stderr))


def sanitize_name(s):
    return ''.join(c for c in s if c.isdigit() or c.isalpha())


class LiveTestCase(TestCase):

    TEST_PATH = __file__

    def __init__(self, *args, **kwargs):
        # don't run as abstract
        if self.TEST_PATH == __file__:
            return
        self.test_base_dir = os.path.split(os.path.abspath(self.TEST_PATH))[0]
        with open(os.path.join(self.test_base_dir,
                               'testconf.yaml'), 'r') as f:
            self.config = yaml.load(f)

        super(LiveTestCase, self).__init__(*args, **kwargs)

    @setup
    def init_hadoop(self):
        self.hadoop_base_dir = None

    @teardown
    def stopteardown_hadoop(self):
        if self.hadoop_base_dir:
            p = Popen([os.path.join(self.hadoop_base_dir,
                                    'bin', 'stop-all.sh')],
                      stdin=PIPE, shell=True)
            p.communicate()

    def start_hadoop(self, version):

        if LooseVersion(version) < LooseVersion('0.20'):
            base_dir = os.environ['HADOOP_HOME_18']
        else:
            base_dir = os.environ['HADOOP_HOME_20']

        os.environ['HADOOP_HOME'] = base_dir

        log.info("    Activating Hadoop version %s" % version)
        self.hadoop_base_dir = base_dir

        try:
            shutil.rmtree('/tmp/hadoop-sjohnson')
        except OSError:
            pass # who cares

        p = Popen([os.path.join(base_dir, 'bin', 'hadoop'), 'namenode', '-format'],
                  stdin=PIPE)
        p.communicate()
        check_call([os.path.join(base_dir, 'bin', 'start-all.sh')],
                   shell=True)

    def conf_info(self, job_data):
        conf_info = job_data.get('config', {})
        conf_info['method'] = conf_info.get('method', 'command_line')
        conf_info['path'] = conf_info.get(
            'path', os.path.join(self.test_base_dir, 'mrjob.conf'))
        return conf_info

    def runnable_test_methods(self):
        for item in self._make_tests():
            yield item

        for item in super(LiveTestCase, self).runnable_test_methods():
            yield item

    def _make_tests(self):
        # don't run as abstract
        if self.TEST_PATH == __file__:
            return

        for python_bin in self.config.get('python_bins', [['python2.6']]):
            #log.info('==== Running jobs for python_bin: %s ====' % python_bin)

            for job in self.config['jobs']:

                given_input = job.get('input', None)
                if given_input is None:
                    given_input = os.path.join(self.test_base_dir, 'input.txt')
                if isinstance(given_input, basestring):
                    input_files = [given_input]
                else:
                    input_files = given_input

                input_files = [os.path.abspath(p) for p in input_files]

                for item in self._test_job(python_bin, job, input_files):
                    yield item

    def _test_job(self, python_bin, job, input_files):
        for runner in self.config['runners']:
            #log.info('  == Running %s in runner: %s ==' % (job['job'], runner))

            # todo: config.method
            conf_info = self.conf_info(job)
            if conf_info['method'] is None:
                config_path = None
                config_args = []
            else:
                config_path = os.path.abspath(conf_info['path'])
                config_args = ['--conf-path', config_path]

            if '-' in runner:
                runner, runner_version = runner.split('-', 2)
                runner_args = [
                    '-r', runner,
                    '--hadoop-version', runner_version
                ]
            else:
                if runner == 'emr':
                    runner_args = ['-r', runner]
                    dummy_runner = EMRJobRunner(conf_path=config_path)
                    runner_version = dummy_runner._opts['hadoop_version']
                elif runner == 'hadoop':
                    runner_args = ['-r', runner]
                    dummy_runner = HadoopJobRunner(conf_path=config_path)
                    runner_version = dummy_runner._opts['hadoop_version']
                elif runner == 'inline':
                    runner_args = ['-r', runner]
                    # make a local one anyway
                    # because inline is dumb about being a dummy
                    dummy_runner = LocalMRJobRunner(conf_path=config_path)
                    runner_version = dummy_runner._opts['hadoop_version']
                elif runner == 'local':
                    runner_args = ['-r', runner]
                    dummy_runner = LocalMRJobRunner(conf_path=config_path)
                    runner_version = dummy_runner._opts['hadoop_version']
                else:
                    raise ValueError('Unknown runner: %s' % runner)

            args = config_args + self.config.get('args', [])
            call_args = (python_bin + ['-m', job['job']] +
                         runner_args +
                         args + input_files)

            name = "test_%s_%s_%s" % (
                sanitize_name('_'.join(python_bin)),
                runner,
                job['job'])

            def tester():
                if runner == 'hadoop':
                    self.start_hadoop(runner_version)
                self._test_job_with_args(job, call_args)

            tester.im_self = self
            tester.im_class = self.__class__
            tester.__name__ = name

            setattr(self, name, tester)
            yield tester

    def _test_job_with_args(self, job, call_args):
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
                                       initial_indent='    ',
                                       subsequent_indent='      '))
            log.info('')

            with open(job.get('output',
                              os.path.join(self.test_base_dir,
                                           'output.txt')),
                      'r') as f:
                a_lines = sorted(f.read().splitlines())
                b_lines = sorted(p.communicate()[0].splitlines())
                assert_equal(a_lines, b_lines)
