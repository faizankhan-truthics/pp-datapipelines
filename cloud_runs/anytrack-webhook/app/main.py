import logging
import os

from flask import Flask, jsonify

from app.webhooks.anytrack import bp as anytrack_bp

logging.basicConfig(level=logging.INFO)


def create_app() -> Flask:
    flask_app = Flask(__name__)
    flask_app.register_blueprint(anytrack_bp)
    # Future webhook sources register here too, e.g.:
    # from app.webhooks.stripe import bp as stripe_bp
    # flask_app.register_blueprint(stripe_bp)

    @flask_app.get("/healthz")
    def healthz():
        return jsonify(status="ok"), 200

    return flask_app


app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
