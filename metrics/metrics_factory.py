from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import boto3
from botocore.client import BaseClient
from statsd import StatsClient


class MetricCreator(ABC):
    """
    The MetricCreator class declares the factory client to return an
    object of a MetricClient class. The Creator's subclasses provide the
    implementation of this method.
    """

    @abstractmethod
    def factory_client(self) -> Any: ...

    def send_collector_crawl_metric(self,
                                    collector_name: str,
                                    env_name: str = 'dev') -> None:
        """
        MetricCreator's primary responsibility is create a common interface to
        send metrics metrics through clients. Subclasses can indirectly change
        that business logic by overriding the factory client method and
        returning a different type of client from it.

        :param str collector_name: Name of collector that increase the metric.
        :param str env_name: Name of environment that is sending the metric.
        :return:
        """
        # Creating a client object.
        metric_client = self.factory_client()

        metric_client.send_collector_metric(
            collector_name,
            env_name
        )


class AwsCreator(MetricCreator):
    """This creator implement a concrete class for aws."""

    def factory_client(self) -> AwsMetricClient:
        return AwsMetricClient()


class StatsDCreator(MetricCreator):
    """This creator implement concrete class for statsd."""

    def factory_client(self) -> StatsDMetricClient:
        return StatsDMetricClient()


class MetricClient(ABC):
    """
    The client interface declares the operations that all concrete clients
    must implement.
    """
    @property
    @abstractmethod
    def client(self) -> Any: ...

    @abstractmethod
    def send_collector_metric(self,
                              collector_name: str,
                              env_name: str = 'dev') -> None:
        """
        Abstract method to standardize metrics delivery across clients.

        :param str collector_name: Name of collector that increase the metric.
        :param str env_name: Environment that is sending the metric.
        :return:
        """
        ...


# noinspection PyAttributeOutsideInit
class AwsMetricClient(MetricClient):
    """This client is the concrete implementation for aws client."""

    @property
    def client(self) -> BaseClient:
        """Returns CloudWatch client."""
        try:
            return self._client  # type: ignore
        except AttributeError:
            self._client = boto3.client('cloudwatch')
            return self._client

    def send_collector_metric(self,
                              collector_name: str,
                              env_name: str = 'dev') -> None:
        """
        Sending of the counter metric via the statsd client.

        :param str collector_name: Name of collector to be used as value.
        :param str env_name: Environment name.
        :return None
        """
        self.client.put_metric_data(
            Namespace='Collector-Metrics',
            MetricData=[
                {
                    'MetricName': 'collector_crawl',
                    'Dimensions': [
                        {
                            'Name': 'collector_name',
                            'Value': collector_name
                        },
                        {
                            'Name': 'environment',
                            'Value': env_name
                        },
                    ],
                    'Timestamp': datetime.now(),
                    'Value': 1,
                    'Unit': 'Count'
                },
            ]
        )


# noinspection PyAttributeOutsideInit
class StatsDMetricClient(MetricClient):
    """This client is the concrete implementation for statsd client."""

    @property
    def client(self) -> StatsClient:
        """Returns statsD client."""
        try:
            return self._client
        except AttributeError:
            self._client = StatsClient()
            return self._client

    def send_collector_metric(self,
                              collector_name: str,
                              env_name: str = 'dev') -> None:
        """
        Sending of the counter metric via the statsd client.

        :param str collector_name: Name of collector to be used as label.
        :param str env_name: Environment name.
        :return None
        """
        self.client.incr(f'collector_crawl.{collector_name}')
