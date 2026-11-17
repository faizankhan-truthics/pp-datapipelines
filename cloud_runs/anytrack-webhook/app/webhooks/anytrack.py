import logging

from flask import Blueprint, jsonify, request

from app.config import ANYTRACK_PRODUCTS, anytrack_expected_token
from app.services.event_processor import write_landing_event

logger = logging.getLogger(__name__)

bp = Blueprint("anytrack", __name__)


@bp.post("/webhooks/anytrack/<product>/<token>")
def anytrack_webhook(product: str, token: str):
    if product not in ANYTRACK_PRODUCTS:
        logger.warning("Unknown AnyTrack product %r", product)
        return jsonify(error="unknown product"), 404

    expected = anytrack_expected_token(product)
    if not expected or token != expected:
        logger.warning("Bad token for AnyTrack product %r", product)
        return jsonify(error="unauthorized"), 401

    raw_body = request.get_data()
    if not raw_body:
        return jsonify(error="empty body"), 400
    logger.info("Get Request from AnyTrack Webhook for %s", product)
    try:
        blob_path = write_landing_event("anytrack", product, raw_body)
    except Exception:
        logger.exception("Failed writing AnyTrack event for %s", product)
        return jsonify(error="storage failure"), 500

    logger.info("Stored AnyTrack event for %s at %s", product, blob_path)
    return jsonify(status="ok"), 200
