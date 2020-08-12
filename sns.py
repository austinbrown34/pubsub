import config.settings as settings
import boto3


class SNSService:
    def __init__(self, aws_endpoint_url=None, aws_access_key_id=None,
                aws_secret_access_key=None, aws_region_name=None):
        self.aws_endpoint_url = aws_endpoint_url if aws_endpoint_url else settings.AWS_ENDPOINT_URL
        self.aws_access_key_id = aws_access_key_id if aws_access_key_id else settings.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = aws_secret_access_key if aws_secret_access_key else settings.AWS_SECRET_ACCESS_KEY
        self.aws_region_name = aws_region_name if aws_region_name else settings.AWS_REGION_NAME
        self.client = boto3.client('sns', aws_access_key_id=self.aws_access_key_id,
                                    aws_secret_access_key=self.aws_secret_access_key,
                                    region_name=self.aws_region_name)
