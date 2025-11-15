""" external_id generation function """
# pylint: disable=all

import uuid


def generate_uuid(name, namespace):
    """Generate a UUID for string input"""
    # use hexa id, email, phone in that order to generate external_id

    # Example usage
    # name = 'me.they@hexa.co.nz'
    # uuid = generate_uuid(name, namespace)
    # print(uuid)

    # Namespace, name as string
    namespace_str = namespace
    name = str(name)

    # Convert the namespace string to a UUID object
    namespace = uuid.UUID(namespace_str)

    # Generate the external_id for the name in uuid v5 format
    external_id = uuid.uuid5(namespace, name)

    # lowercase uuid
    external_id = str(external_id).lower()

    return external_id
