''' hash a uuid v5 '''

import uuid
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Generate a new UUID to use as our namespace
MY_NAMESPACE = uuid.uuid4()
logging.info(MY_NAMESPACE)
# ee7c9503-34e9-41b5-8555-ca9d738869d2

# Namespace and name
namespace_str = 'ee7c9503-34e9-41b5-8555-ca9d738869d2'
email = '123'

# Convert the string to a UUID object
namespace = uuid.UUID(namespace_str)

uuid = uuid.uuid5(namespace, email)

logging.info(uuid)
logging.info(uuid.hex)
# 2023-06-02 06:16:21,470 - INFO - 2b0a02ff-4430-59a3-8beb-2056d21d5690
# 2023-06-02 06:16:21,470 - INFO - 2b0a02ff443059a38beb2056d21d5690
