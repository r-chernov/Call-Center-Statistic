from flask import Blueprint, jsonify, request

amo_bp = Blueprint("amo", __name__, url_prefix="/amo")


@amo_bp.route("/callback")
def amo_callback():
    code = request.args.get("code")
    state = request.args.get("state")
    error = request.args.get("error")
    error_description = request.args.get("error_description")
    return jsonify({
        "ok": error is None,
        "code": code,
        "state": state,
        "error": error,
        "error_description": error_description
    })
