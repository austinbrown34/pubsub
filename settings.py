import os
from os.path import join
from dotenv import load_dotenv
import sys

# determine if application is a script file or frozen exe
if getattr(sys, 'frozen', False):
    application_path = os.path.dirname(sys.executable)
else:
    application_path = os.path.dirname(__file__)


dotenv_path = join(application_path, '.env')
load_dotenv(dotenv_path)

user = os.getenv('MESSAGE_DB_USERNAME')

# Using variables.
print(f'{user} starting pubsub')
