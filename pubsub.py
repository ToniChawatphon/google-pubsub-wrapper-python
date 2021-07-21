import logging
from typing import Callable
from google.cloud import pubsub


class PubSubPublisher:
    def __init__(self, project_id: str, topic_id: str):
        self.project_id       = project_id
        self.topic_id         = topic_id
        self.topic_path       = ""
        self.publisher_client = None
    
    def conn(self):
        """Connect to PubSub Publisher
        """
        try:
            self.publisher_client = pubsub.PublisherClient()
            self.topic_path       = self.publisher_client.topic_path(self.project_id, self.topic_id)
            logging.info("- connected to pub/sub publisher client")        
            return self.publisher_client
        except Exception as e:
            logging.error('could not connect to pub/sub publisher client')    
            raise Exception(f'could not connect to pub/sub publisher client, {str(e)}')
    
    def set_new_topic(self, project_id: str, topic_name: str):
        if not self.publisher_client:
            raise Exception('No PubSub connection')
        self.project_id = project_id
        self.topic_name = topic_name
        self.topic_path = self._get_topic_path(project_id, topic_name)

    def publish_message(self, message: str, **attributes):
        """Publishes multiple messages to a Pub/Sub topic
        :param message (str) pubsub message
        """
        if not self.publisher_client:
            raise Exception('No PubSub connection')
            
        encoded_data = message.encode('utf-8')
        future = self.publisher_client.publish(self.topic_path, data=encoded_data, attrs=attributes)
        logging.info("- published message {}".format(future.result()))
    
    @staticmethod
    def _get_topic_path(project_id: str, topic: str) -> str:
        topic_path = f'projects/{project_id}/topics/{topic}'
        return topic_path

class PubSubSubscriber:
    project_id: str
    subscription_name: str

    def __init__(self, project_id: str, subscription_name: str):
        self.project_id        = project_id
        self.subscription_name = subscription_name
        self.subscription_path = ""
        self.subscriber_client = None

    def conn(self):
        """Connect to PubSub Subscriber
        """
        try:
            self.subscriber_client = pubsub.SubscriberClient()
            self.subscription_path = self.subscriber_client.subscription_path(self.project_id, self.subscription_name)
            logging.info("- connected to pub/sub subscriber client")        
            return self.subscriber_client
        except Exception as e:
            logging.error('could not connect to pub/sub subscriber client')
            raise Exception(f'could not connect to pub/sub subscriber client, {str(e)}')

    def pull_message(self, callback_method: Callable, timeout: int = None, max_messages: int = None):
        """Pull message from pubsub subscription
        :param callback_method (Callable) A function that will be processed after receiving message
        :param timeout         (int)      Pub/Sub subscription timeout
        :param max_messages    (int)      Number of maximum message of pulling at once
        """
        if not self.subscriber_client:
            raise Exception('No PubSub connection')

        flow_control = ()
        if max_messages is not None:
            if isinstance(max_messages, int):
                flow_control = pubsub.types.FlowControl(max_messages=max_messages)

        streaming_pull_future = self.subscriber_client.subscribe(
                                            subscription=self.subscription_path, 
                                            callback=callback_method,
                                            flow_control=flow_control
                                        )
        logging.info("Listening messages on {} ...".format(self.subscription_path))
        with self.subscriber_client:
            try:
                streaming_pull_future.result(timeout=timeout)
            except Exception as ex:
                streaming_pull_future.cancel()
                logging.error(ex)
    
    @staticmethod
    def callback(message):
        """Callback message example
        :param message (str)
        """
        logging.info("received message: {}".format(message.data))
        if message.attributes:
            logging.info("attributes:")
            for key in message.attributes:
                value = message.attributes.get(key)
                logging.info("- {}: {}".format(key, value))
        message.ack()
