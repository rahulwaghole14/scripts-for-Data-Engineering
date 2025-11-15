"""
classifying business names and personal names
"""
import os
import spacy
from common.logging.logger import logger

# installation
# pip install spacy
# pip install https://github.com/explosion/spacy-models/releases/download \
# /en_core_web_sm-3.7.1/en_core_web_sm-3.7.1-py3-none-any.whl

logger = logger(
    "classification",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

# load the model once only to save time
nlp = spacy.load("en_core_web_sm")


def get_entity_info(name, nlp_model):

    """Get entity information from a
    given name using SpaCy
    return entities list of tuples with text and label
    label: PERSON, ORG, GPE, etc.
    """

    try:
        # Load SpaCy small model
        doc = nlp_model(name)
        entities = [(ent.text, ent.label_) for ent in doc.ents]
        # return entities list of tuples
    except Exception as e:  # pylint: disable=broad-except
        logger.error("get_entity_info() Failed to get entity info: %s", e)
    return entities
