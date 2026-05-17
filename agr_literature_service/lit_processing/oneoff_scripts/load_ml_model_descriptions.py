"""Load ml_model descriptions from TSV and update parameters for WB entity extractors.

This script:
1. Loads descriptions from ml_model_descriptions.tsv into the ml_model.description column
2. Parses TF-IDF Threshold, Min Matches, Match Uppercase from descriptions
   for the 5 WB entity extractors and updates their parameters column with JSON
"""
import json
import logging
import re

from sqlalchemy.orm import Session

from agr_literature_service.api.models.ml_model_model import MLModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Path to the TSV file containing ml_model descriptions
TSV_FILE_PATH = "../agr_automated_information_extraction/docs/ml_model_descriptions.tsv"

# WB Entity Extractor ml_model_ids that need parameters extracted
WB_ENTITY_EXTRACTOR_IDS = [29, 31, 32, 44, 52]


def parse_entity_extractor_params(description: str) -> dict:
    """Extract TF-IDF Threshold, Min Matches, and Match Uppercase from description.

    Args:
        description: The description text containing parameters

    Returns:
        Dict with tfidf_threshold, min_matches, match_uppercase or empty dict if not found
    """
    params = {}

    # Extract TF-IDF Threshold
    tfidf_match = re.search(r'TF-IDF Threshold:\s*([\d.]+)', description)
    if tfidf_match:
        params['tfidf_threshold'] = float(tfidf_match.group(1))

    # Extract Min Matches
    min_match = re.search(r'Min Matches:\s*(\d+)', description)
    if min_match:
        params['min_matches'] = int(min_match.group(1))

    # Extract Match Uppercase
    uppercase_match = re.search(r'Match Uppercase:\s*(True|False)', description)
    if uppercase_match:
        params['match_uppercase'] = uppercase_match.group(1) == 'True'

    return params


def load_descriptions():
    """Load descriptions from TSV file and update ml_model table."""
    db_session: Session = create_postgres_session(False)

    try:
        # Read TSV file
        descriptions = {}
        with open(TSV_FILE_PATH, 'r') as f:
            # Skip header
            next(f)
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 6:
                    try:
                        ml_model_id = int(parts[0])
                        description = parts[5]
                        descriptions[ml_model_id] = description
                    except ValueError:
                        logger.warning("Could not parse ml_model_id from line: %s", line[:50])

        logger.info("Loaded %d descriptions from TSV file", len(descriptions))

        # Update descriptions for all models
        desc_updated = 0
        for ml_model_id, description in descriptions.items():
            model = db_session.query(MLModel).filter(
                MLModel.ml_model_id == ml_model_id
            ).first()

            if model:
                model.description = description
                desc_updated += 1
                logger.debug("Updated description for ml_model_id=%d", ml_model_id)
            else:
                logger.warning("ml_model_id=%d not found in database", ml_model_id)

        logger.info("Updated descriptions for %d ml_model rows", desc_updated)

        # Update parameters for WB entity extractors
        params_updated = 0
        for ml_model_id in WB_ENTITY_EXTRACTOR_IDS:
            if ml_model_id not in descriptions:
                logger.warning(
                    "ml_model_id=%d not found in TSV, skipping parameters update",
                    ml_model_id
                )
                continue

            description = descriptions[ml_model_id]
            params = parse_entity_extractor_params(description)

            if params:
                model = db_session.query(MLModel).filter(
                    MLModel.ml_model_id == ml_model_id
                ).first()

                if model:
                    params_json = json.dumps(params)
                    model.parameters = params_json
                    params_updated += 1
                    logger.info(
                        "Updated parameters for ml_model_id=%d: %s",
                        ml_model_id, params_json
                    )
            else:
                logger.warning(
                    "Could not parse parameters from description for ml_model_id=%d",
                    ml_model_id
                )

        db_session.commit()
        logger.info("Updated parameters for %d WB entity extractors", params_updated)

        # Report the results
        for ml_model_id in WB_ENTITY_EXTRACTOR_IDS:
            model = db_session.query(MLModel).filter(
                MLModel.ml_model_id == ml_model_id
            ).first()

            if model:
                logger.info(
                    "ml_model_id=%d topic=%s parameters=%s",
                    model.ml_model_id, model.topic, model.parameters
                )

    except Exception as e:
        logger.error("Error during update: %s", e)
        db_session.rollback()
        raise
    finally:
        db_session.close()


if __name__ == "__main__":
    load_descriptions()
