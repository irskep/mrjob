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
from StringIO import StringIO

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

import boto

from mrjob.emr import EMRJobRunner
from mrjob.util import log_to_stream
from tests.quiet import no_handlers_for_logger


class EC2InstanceGroupLiveTestCase(unittest.TestCase):
    """Live mirror of tests.emr_test.EC2InstanceGroupTestCase"""

    def _test_instance_groups(self, opts, **expected):
        """Run a job with the given option dictionary, and check for
        for instance, number, and optional bid price for each instance role.

        Specify expected instance group info like:

        <role>=(num_instances, instance_type, bid_price)
        """
        runner = EMRJobRunner(**opts)
        job_flow_id = runner.make_persistent_job_flow()
        job_flow = runner.make_emr_conn().describe_jobflow(job_flow_id)

        # convert expected to a dict of dicts
        role_to_expected = {}
        for role, (num, instance_type, bid_price) in expected.iteritems():
            info = {
                'instancerequestcount': str(num),
                'instancetype': instance_type,
            }
            if bid_price:
                info['market'] = 'SPOT'
                info['bidprice'] = bid_price
            else:
                info['market'] = 'ON_DEMAND'

            role_to_expected[role.upper()] = info

        # convert actual instance groups to dicts
        role_to_actual = {}
        for ig in job_flow.instancegroups:
            info = {}
            for field in ('bidprice', 'instancerequestcount',
                          'instancetype', 'market'):
                if hasattr(ig, field):
                    info[field] = getattr(ig, field)
            role_to_actual[ig.instancerole] = info

        self.assertEqual(role_to_expected, role_to_actual)

        # also check master/slave and # of instance types
        expected_master_instance_type = role_to_expected.get(
            'MASTER', {}).get('instancetype')
        self.assertEqual(expected_master_instance_type,
                         getattr(job_flow, 'masterinstancetype', None))

        expected_slave_instance_type = role_to_expected.get(
            'CORE', {}).get('instancetype')
        self.assertEqual(expected_slave_instance_type,
                         getattr(job_flow, 'slaveinstancetype', None))

        expected_instance_count = str(sum(
            int(info['instancerequestcount'])
            for info in role_to_expected.itervalues()))
        self.assertEqual(expected_instance_count, job_flow.instancecount)

        runner.make_emr_conn().terminate_jobflow(job_flow_id)

    def test_defaults(self):
        self._test_instance_groups(
            {},
            master=(1, 'm1.small', None))

        self._test_instance_groups(
            {'num_ec2_instances': 3},
            core=(2, 'm1.small', None),
            master=(1, 'm1.small', None))

    def test_single_instance(self):
        self._test_instance_groups(
            {'ec2_instance_type': 'c1.xlarge'},
            master=(1, 'c1.xlarge', None))

    def test_multiple_instances(self):
        self._test_instance_groups(
            {'ec2_instance_type': 'c1.xlarge', 'num_ec2_instances': 3},
            core=(2, 'c1.xlarge', None),
            master=(1, 'm1.small', None))

    def test_explicit_master_and_slave_instance_types(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large'},
            master=(1, 'm1.large', None))

        self._test_instance_groups(
            {'ec2_slave_instance_type': 'm2.xlarge',
             'num_ec2_instances': 3},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.small', None))

        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'm2.xlarge',
             'num_ec2_instances': 3},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.large', None))

    def test_explicit_instance_types_take_precedence(self):
        self._test_instance_groups(
            {'ec2_instance_type': 'c1.xlarge',
             'ec2_master_instance_type': 'm1.large'},
            master=(1, 'm1.large', None))

        self._test_instance_groups(
            {'ec2_instance_type': 'c1.xlarge',
             'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'm2.xlarge',
             'num_ec2_instances': 3},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.large', None))

    def test_zero_core_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'c1.medium',
             'num_ec2_core_instances': 0},
            master=(1, 'c1.medium', None))

    def test_core_spot_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_core_instance_type': 'c1.medium',
             'ec2_core_instance_bid_price': '0.20',
             'num_ec2_core_instances': 5},
            core=(5, 'c1.medium', '0.20'),
            master=(1, 'm1.large', None))

    def test_core_on_demand_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_core_instance_type': 'c1.medium',
             'num_ec2_core_instances': 5},
            core=(5, 'c1.medium', None),
            master=(1, 'm1.large', None))

        # Test the ec2_slave_instance_type alias
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'c1.medium',
             'num_ec2_instances': 6},
            core=(5, 'c1.medium', None),
            master=(1, 'm1.large', None))

    def test_core_and_task_on_demand_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_core_instance_type': 'c1.medium',
             'num_ec2_core_instances': 5,
             'ec2_task_instance_type': 'm2.xlarge',
             'num_ec2_task_instances': 20,
             },
            core=(5, 'c1.medium', None),
            master=(1, 'm1.large', None),
            task=(20, 'm2.xlarge', None))

    def test_core_and_task_spot_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_core_instance_type': 'c1.medium',
             'ec2_core_instance_bid_price': '0.20',
             'num_ec2_core_instances': 10,
             'ec2_task_instance_type': 'm2.xlarge',
             'ec2_task_instance_bid_price': '1.00',
             'num_ec2_task_instances': 20,
             },
            core=(10, 'c1.medium', '0.20'),
            master=(1, 'm1.large', None),
            task=(20, 'm2.xlarge', '1.00'))

        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_core_instance_type': 'c1.medium',
             'num_ec2_core_instances': 10,
             'ec2_task_instance_type': 'm2.xlarge',
             'ec2_task_instance_bid_price': '1.00',
             'num_ec2_task_instances': 20,
             },
            core=(10, 'c1.medium', None),
            master=(1, 'm1.large', None),
            task=(20, 'm2.xlarge', '1.00'))

    def test_master_and_core_spot_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_master_instance_bid_price': '0.50',
             'ec2_core_instance_type': 'c1.medium',
             'ec2_core_instance_bid_price': '0.20',
             'num_ec2_core_instances': 10,
             },
            core=(10, 'c1.medium', '0.20'),
            master=(1, 'm1.large', '0.50'))

    def test_master_spot_instance(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_master_instance_bid_price': '0.50',
             },
            master=(1, 'm1.large', '0.50'))

    def test_zero_or_blank_bid_price_means_on_demand(self):
        self._test_instance_groups(
            {'ec2_master_instance_bid_price': '0',
             },
            master=(1, 'm1.small', None))

        self._test_instance_groups(
            {'num_ec2_core_instances': 3,
             'ec2_core_instance_bid_price': '0.00',
             },
            core=(3, 'm1.small', None),
            master=(1, 'm1.small', None))

        self._test_instance_groups(
            {'num_ec2_core_instances': 3,
             'num_ec2_task_instances': 5,
             'ec2_task_instance_bid_price': '',
             },
            core=(3, 'm1.small', None),
            master=(1, 'm1.small', None),
            task=(5, 'm1.small', None))

    def test_pass_invalid_bid_prices_through_to_emr(self):
        self.assertRaises(
            boto.exception.EmrResponseError,
            self._test_instance_groups,
            {'ec2_master_instance_bid_price': 'all the gold in California'})

    def test_task_type_defaults_to_core_type(self):
        self._test_instance_groups(
            {'ec2_core_instance_type': 'c1.medium',
             'num_ec2_core_instances': 5,
             'num_ec2_task_instances': 20,
             },
            core=(5, 'c1.medium', None),
            master=(1, 'm1.small', None),
            task=(20, 'c1.medium', None))

    def test_mixing_instance_number_opts_on_cmd_line(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.emr', stderr)
            self._test_instance_groups(
                {'num_ec2_instances': 4,
                 'num_ec2_core_instances': 10},
                core=(10, 'm1.small', None),
                master=(1, 'm1.small', None))

        self.assertIn('does not make sense', stderr.getvalue())
