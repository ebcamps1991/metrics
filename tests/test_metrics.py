import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from metrics.metrics_factory import (
    AwsCreator, StatsDCreator, AwsMetricClient, StatsDMetricClient
)


class TestMetricsFactory(unittest.TestCase):
    """Test MetricsFactory"""

    def setUp(self) -> None:
        self._metric_client = None

    def test_aws_client_get_client_success(self):
        metric_creator = AwsCreator()

        self.assertIsInstance(
            metric_creator.factory_client(),
            AwsMetricClient
        )

    def test_statsd_client_get_client_success(self):
        metric_creator = StatsDCreator()

        self.assertIsInstance(
            metric_creator.factory_client(),
            StatsDMetricClient
        )

    @patch.dict(os.environ, {'AWS_DEFAULT_REGION': 'us-east-1'})
    @patch('metrics.metrics_factory.datetime')
    @patch('metrics.metrics_factory.AwsMetricClient.client')
    def test_aws_client_send_correct_message(self,
                                             client_mock: MagicMock,
                                             datetime_mock: MagicMock) -> None:
        self._metric_client = AwsMetricClient()

        # Mocking now, never will be the same.
        current_moment = datetime(2022, 3, 15, 18, 28, 42, 809605)
        datetime_mock.now.return_value = current_moment

        self._metric_client.send_collector_metric(
            collector_name='gitlab_crawl'
        )

        client_mock.put_metric_data.assert_called_once_with(
            Namespace='Collector-Metrics',
            MetricData=[
                {
                    'MetricName': 'collector_crawl',
                    'Dimensions': [
                        {
                            'Name': 'collector_name',
                            'Value': 'gitlab_crawl'
                        },
                        {
                            'Name': 'environment',
                            'Value': 'dev'
                        },
                    ],
                    'Timestamp': current_moment,
                    'Value': 1,
                    'Unit': 'Count'
                },
            ]
        )

    @patch('metrics.metrics_factory.StatsDMetricClient.client')
    def test_statsd_instance_client_correct(self,
                                            client_mock: MagicMock) -> None:
        self._metric_client = StatsDMetricClient()

        self._metric_client.send_collector_metric(
            collector_name='gitlab_crawl'
        )

        parameter = 'collector_crawl.gitlab_crawl'

        client_mock.incr.assert_called_once_with(parameter)

