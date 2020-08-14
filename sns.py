import config.settings as settings
import boto3
import json


class SNSService:
    def __init__(self, aws_endpoint_url=None, aws_access_key_id=None,
                aws_secret_access_key=None, aws_region_name=None):
        self.aws_endpoint_url = aws_endpoint_url if aws_endpoint_url else settings.AWS_ENDPOINT_URL
        self.aws_access_key_id = aws_access_key_id if aws_access_key_id else settings.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = aws_secret_access_key if aws_secret_access_key else settings.AWS_SECRET_ACCESS_KEY
        self.aws_region_name = aws_region_name if aws_region_name else settings.AWS_REGION_NAME
        self.client = boto3.client('sns', endpoint_url=self.aws_endpoint_url,
                                    aws_access_key_id=self.aws_access_key_id,
                                    aws_secret_access_key=self.aws_secret_access_key,
                                    region_name=self.aws_region_name)

    def create_topic(self, topic_name):
        response = self.client.create_topic(
            Name=topic_name
        )
        return response

    def delete_topic(self, topic_arn):
        response = self.client.delete_topic(
            TopicArn=topic_arn
        )
        return response

    def get_subscription_attributes(self, subscription_arn):
        response = self.client.get_subscription_attributes(
            SubscriptionArn=subscription_arn
        )
        return response

    def get_topic_attributes(self, topic_arn):
        response = self.client.get_topic_attributes(
            TopicArn=topic_arn
        )
        return response

    def list_subscriptions(self, next_token=None):
        params = {
            'NextToken': next_token
        }
        response = self.client.list_subscriptions(
            **{k: v for k, v in params.items() if v is not None}
        )
        return response

    def list_subscriptions_by_topic(self, topic_arn, next_token=None):
        params = {
            'TopicArn': topic_arn,
            'NextToken': next_token
        }
        response = self.client.list_subscriptions_by_topic(
            **{k: v for k, v in params.items() if v is not None}
        )
        return response

    def list_topics(self, next_token=None):
        params = {
            'NextToken': next_token
        }
        response = self.client.list_topics(
            **{k: v for k, v in params.items() if v is not None}
        )
        return response

    def publish(self, topic_arn, message, target_arn=None, subject=None,
                message_structure=None, message_attributes=None, phone=None):
        if type(message) == dict:
            message = json.dumps(message)
        params = {
            'PhoneNumber': phone,
            'Message': message,
            'Subject': subject,
            'MessageStructure': message_structure,
            'MessageAttributes': message_attributes
        }
        if topic_arn:
            params['TopicArn'] = topic_arn
        else:
            params['TargetArn'] = target_arn

        response = self.client.publish(
            **{k: v for k, v in params.items() if v is not None}
        )
        return response

    def set_subscription_attributes(self, subscription_arn, attribute_name,
                                    attribute_value):
        response = self.client.set_subscription_attributes(
            SubscriptionArn=subscription_arn,
            AttributeName=attribute_name,
            AttributeValue=attribute_value
        )
        return response

    def set_topic_attributes(self, topic_arn, attribute_name,
                                    attribute_value):
        response = self.client.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName=attribute_name,
            AttributeValue=attribute_value
        )
        return response

    def subscribe(self, topic_arn, protocol, endpoint=None, attributes=None,
                  return_subscription_arn=False):
        params = {
            'TopicArn': topic_arn,
            'Protocol': protocol,
            'Endpoint': endpoint,
            'Attributes': attributes,
            'ReturnSubscriptionArn': return_subscription_arn
        }
        response = self.client.subscribe(
            **{k: v for k, v in params.items() if v is not None}
        )
        return response

    def add_permission(self, topic_arn, label, aws_account_id, action_name):
        response = self.client.add_permission(
            TopicArn=topic_arn,
            Label=label,
            AWSAccountId=aws_account_id,
            ActionName=action_name
        )
        return response

    def unsubscribe(self, subscription_arn):
        response = self.client.unsubscribe(
            SubscriptionArn=subscription_arn
        )
        return response
