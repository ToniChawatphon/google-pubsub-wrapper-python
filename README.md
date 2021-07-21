# google-pubsub-wrapper-python
Google PubSub wrapper in python, consisting of Subscriber and Publisher
## author
- ToniChawatphon

# Setup 
```
pip install pipenv
```
```
pipenv install
```

## How to use
### 1. Example of Pub/Sub Publisher

```python
from pubsub import PubSubPublisher


# initiate instance
pubsub = PubSubPublisher(project_id="project_id",
                        topic_id="topic_id"
                    )

# make your first connection
pubsub.conn()

# publish message to pubsub topic, including attributes
message = "Hi PubSub"
pubsub.publish_message(message
                      ATTR1='attribute_1'
                      ATTR2='attribute_2'
                    )
```  

### 2. Example of Pub/Sub Subscriber

```python
from pubsub import PubSubSubscriber


def callback(message):
    # # TODO Do something
    # print(message)

    # # delete message after processing data
    # message.ack()
    pass


# initiate instance
pubsub = PubSubSubscriber(project_id="project_id",
                        subscription_name="subscription_name"
                    )

# make your first connection
pubsub.conn()

# publish message to pubsub topic, including attributes
pubsub.pull_message(callback_method=callback, 
                    timeout=120
                )
```