import os
import time
import threading
from functools import wraps
from datetime import datetime

import boto3
import jwt
from boto3.dynamodb.conditions import Key
from botocore.exceptions import BotoCoreError, ClientError
from flask import Flask, jsonify, request, make_response

app = Flask(__name__)

AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "arxiv-papers")
SECRET_KEY = os.environ.get("SECRET_KEY", "your-secret-key")
TOKEN_EXPIRES_IN = 3600

USERS = {
    "admin": "secret",
    "viewer": "readonly",
}

USER_ROLES = {
    "admin": "admin",
    "viewer": "viewer",
}

_start_time = time.time()
_stats_lock = threading.Lock()
_request_stats = {
    "total": 0,
    "by_status": {},
}

# If you need local DynamoDB, set DYNAMODB_ENDPOINT.
_dynamodb_kwargs = {"region_name": AWS_REGION}
_dynamodb_endpoint = os.environ.get("DYNAMODB_ENDPOINT")
if _dynamodb_endpoint:
    _dynamodb_kwargs["endpoint_url"] = _dynamodb_endpoint

dynamodb = boto3.resource("dynamodb", **_dynamodb_kwargs)
table = dynamodb.Table(DYNAMODB_TABLE)


def json_error(status_code, message, **extra):
    payload = {"error": message}
    payload.update(extra)
    return jsonify(payload), status_code


def _increment_stats(status_code):
    with _stats_lock:
        _request_stats["total"] += 1
        code = str(status_code)
        _request_stats["by_status"][code] = _request_stats["by_status"].get(code, 0) + 1


@app.after_request
def track_requests(response):
    _increment_stats(response.status_code)
    return response


@app.errorhandler(404)
def handle_404(_):
    return json_error(404, "Resource not found")


@app.errorhandler(500)
def handle_500(_):
    return json_error(500, "Server error")


def create_token(username):
    now = int(time.time())
    payload = {
        "sub": username,
        "iat": now,
        "exp": now + TOKEN_EXPIRES_IN,
        "role": USER_ROLES.get(username, "viewer"),
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token


def decode_token(token):
    return jwt.decode(token, SECRET_KEY, algorithms=["HS256"])


def require_auth(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return json_error(401, "Missing or invalid token")

        token = auth_header.split(" ", 1)[1].strip()
        if not token:
            return json_error(401, "Missing or invalid token")

        try:
            request.jwt_payload = decode_token(token)
        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
            return json_error(401, "Missing or invalid token")

        return view_func(*args, **kwargs)

    return wrapper


def normalize_list_value(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",")]
        return [p for p in parts if p]
    return [str(value)]


def strip_prefix(value):
    text = str(value)
    if "#" in text:
        return text.split("#", 1)[1]
    return text


def extract_date_component(value):
    if value is None:
        return ""
    text = str(value).strip()
    if "#" in text:
        text = text.split("#", 1)[1]
    if "T" in text:
        text = text.split("T", 1)[0]
    return text[:10]


def normalize_paper(item):
    arxiv_id = (
        item.get("arxiv_id")
        or item.get("paper_id")
        or item.get("id")
        or item.get("PaperId")
        or item.get("GSI2PK")
        or strip_prefix(item.get("PK", ""))
        or ""
    )

    categories = normalize_list_value(item.get("categories"))
    if not categories and item.get("category"):
        categories = [str(item.get("category"))]

    authors = normalize_list_value(item.get("authors"))
    if not authors and item.get("author"):
        authors = [str(item.get("author"))]

    published = (
        item.get("published")
        or item.get("date")
        or item.get("created_at")
        or extract_date_component(item.get("SK"))
        or ""
    )

    return {
        "arxiv_id": str(arxiv_id),
        "title": item.get("title", ""),
        "authors": authors,
        "abstract": item.get("abstract", ""),
        "categories": categories,
        "published": str(published),
    }


def normalize_paper_summary(item):
    paper = normalize_paper(item)
    return {
        "arxiv_id": paper["arxiv_id"],
        "title": paper["title"],
        "authors": paper["authors"],
        "published": paper["published"],
    }


def query_table(**kwargs):
    response = table.query(**kwargs)
    return response.get("Items", [])


def scan_all_items():
    items = []
    kwargs = {}
    while True:
        response = table.scan(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def query_by_index(index_name, key_name, key_value, limit=None, scan_forward=False):
    params = {
        "IndexName": index_name,
        "KeyConditionExpression": Key(key_name).eq(key_value),
        "ScanIndexForward": scan_forward,
    }
    if limit is not None:
        params["Limit"] = limit
    return query_table(**params)


def category_matches_item(item, category):
    pk = str(item.get("PK", ""))
    category_field = item.get("category")
    categories_field = normalize_list_value(item.get("categories"))

    if category_field == category:
        return True
    if category in categories_field:
        return True
    if pk == category:
        return True
    if pk == f"CATEGORY#{category}":
        return True
    if pk == f"category#{category}":
        return True
    if pk.endswith(f"#{category}"):
        return True
    return False


def date_matches_item(item, start, end):
    published = item.get("published") or item.get("date") or item.get("created_at")
    if published:
        published_date = extract_date_component(published)
        if start <= published_date <= end:
            return True

    sk = item.get("SK")
    if sk:
        sk_date = extract_date_component(sk)
        if start <= sk_date <= end:
            return True

    return False


def sort_items_by_date_desc(items):
    def sort_key(item):
        published = item.get("published") or item.get("date") or item.get("created_at")
        if published:
            return extract_date_component(published)
        sk = item.get("SK")
        if sk:
            return extract_date_component(sk)
        return ""

    return sorted(items, key=sort_key, reverse=True)


def get_items_by_category(category, limit):
    pk_candidates = [
        category,
        f"CATEGORY#{category}",
        f"category#{category}",
        f"CAT#{category}",
    ]

    for pk_value in pk_candidates:
        try:
            items = query_table(
                KeyConditionExpression=Key("PK").eq(pk_value),
                ScanIndexForward=False,
                Limit=limit,
            )
            filtered = [item for item in items if category_matches_item(item, category)]
            if filtered:
                return filtered[:limit]
        except (ClientError, BotoCoreError):
            continue

    all_items = scan_all_items()
    filtered = [item for item in all_items if category_matches_item(item, category)]
    filtered = sort_items_by_date_desc(filtered)
    return filtered[:limit]


def get_item_by_arxiv_id(arxiv_id):
    try:
        items = query_by_index("PaperIdIndex", "GSI2PK", arxiv_id, limit=1)
        if items:
            return items[0]
    except (ClientError, BotoCoreError):
        pass

    all_items = scan_all_items()
    for item in all_items:
        candidate_ids = {
            str(item.get("arxiv_id", "")),
            str(item.get("paper_id", "")),
            str(item.get("id", "")),
            str(item.get("PaperId", "")),
            str(item.get("GSI2PK", "")),
            str(item.get("PK", "")),
        }
        if arxiv_id in candidate_ids:
            return item

    return None


def get_items_by_date_range(category, start, end):
    pk_candidates = [
        category,
        f"CATEGORY#{category}",
        f"category#{category}",
        f"CAT#{category}",
    ]
    sk_candidates = [
        (start, end),
        (f"{start}T00:00:00Z", f"{end}T23:59:59Z"),
        (f"DATE#{start}", f"DATE#{end}"),
        (f"date#{start}", f"date#{end}"),
    ]

    for pk_value in pk_candidates:
        for sk_start, sk_end in sk_candidates:
            try:
                items = query_table(
                    KeyConditionExpression=Key("PK").eq(pk_value) & Key("SK").between(sk_start, sk_end),
                    ScanIndexForward=False,
                )
                filtered = [
                    item for item in items
                    if category_matches_item(item, category) and date_matches_item(item, start, end)
                ]
                if filtered:
                    return filtered
            except (ClientError, BotoCoreError):
                continue

    all_items = scan_all_items()
    filtered = [
        item for item in all_items
        if category_matches_item(item, category) and date_matches_item(item, start, end)
    ]
    return sort_items_by_date_desc(filtered)


def get_items_by_author(author_name):
    try:
        items = query_by_index("AuthorIndex", "GSI1PK", author_name)
        if items:
            return items
    except (ClientError, BotoCoreError):
        pass

    all_items = scan_all_items()
    filtered = []
    for item in all_items:
        if item.get("author") == author_name:
            filtered.append(item)
            continue
        authors = normalize_list_value(item.get("authors"))
        if author_name in authors:
            filtered.append(item)
            continue
        if author_name and author_name.lower() in str(item.get("authors", "")).lower():
            filtered.append(item)
    return filtered


def get_items_by_keyword(keyword, limit=20):
    try:
        items = query_by_index("KeywordIndex", "GSI3PK", keyword, limit=limit)
        if items:
            return items[:limit]
    except (ClientError, BotoCoreError):
        pass

    all_items = scan_all_items()
    filtered = []
    for item in all_items:
        keywords = normalize_list_value(item.get("keywords"))
        if keyword in keywords:
            filtered.append(item)
            continue
        if item.get("keyword") == keyword:
            filtered.append(item)
            continue
        if keyword and keyword.lower() in str(item.get("keywords", "")).lower():
            filtered.append(item)

    filtered = sort_items_by_date_desc(filtered)
    return filtered[:limit]


@app.route("/api/stats", methods=["GET"])
def api_stats():
    with _stats_lock:
        stats_copy = {
            "total": _request_stats["total"],
            "by_status": dict(_request_stats["by_status"]),
        }

    return jsonify({
        "status": "healthy",
        "uptime_seconds": int(time.time() - _start_time),
        "region": AWS_REGION,
        "table": DYNAMODB_TABLE,
        "requests": stats_copy,
    })


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return json_error(400, "Missing or invalid parameters")

    if USERS.get(username) != password:
        return json_error(401, "Invalid credentials")

    token = create_token(username)
    return jsonify({
        "token": token,
        "expires_in": TOKEN_EXPIRES_IN,
    })


@app.route("/api/papers", methods=["GET"])
@require_auth
def api_papers():
    category = request.args.get("category")
    limit_raw = request.args.get("limit", "20")

    if not category:
        return json_error(400, "Missing or invalid parameters")

    try:
        limit = int(limit_raw)
        if limit <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return json_error(400, "Missing or invalid parameters")

    try:
        items = get_items_by_category(category, limit)
    except (ClientError, BotoCoreError):
        app.logger.exception("Query /api/papers failed")
        return json_error(500, "Server error")

    papers = [normalize_paper_summary(item) for item in items[:limit]]
    return jsonify({
        "category": category,
        "papers": papers,
        "count": len(papers),
    })


@app.route("/api/papers/<string:arxiv_id>", methods=["GET"])
@require_auth
def api_paper_by_id(arxiv_id):
    try:
        item = get_item_by_arxiv_id(arxiv_id)
    except (ClientError, BotoCoreError):
        app.logger.exception("Query /api/papers/<arxiv_id> failed")
        return json_error(500, "Server error")

    if not item:
        return json_error(404, "Paper not found", arxiv_id=arxiv_id)

    paper = normalize_paper(item)
    return jsonify({
        "arxiv_id": paper["arxiv_id"],
        "title": paper["title"],
        "authors": paper["authors"],
        "abstract": paper["abstract"],
        "categories": paper["categories"],
        "published": paper["published"],
    })


@app.route("/api/papers/search", methods=["GET"])
@require_auth
def api_papers_search():
    category = request.args.get("category")
    start = request.args.get("start")
    end = request.args.get("end")

    if not category or not start or not end:
        return json_error(400, "Missing or invalid parameters")

    try:
        datetime.strptime(start, "%Y-%m-%d")
        datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        return json_error(400, "Missing or invalid parameters")

    try:
        items = get_items_by_date_range(category, start, end)
    except (ClientError, BotoCoreError):
        app.logger.exception("Query /api/papers/search failed")
        return json_error(500, "Server error")

    papers = [normalize_paper_summary(item) for item in items]
    return jsonify({
        "category": category,
        "start": start,
        "end": end,
        "papers": papers,
        "count": len(papers),
    })


@app.route("/api/papers/author/<path:author_name>", methods=["GET"])
@require_auth
def api_papers_by_author(author_name):
    try:
        items = get_items_by_author(author_name)
    except (ClientError, BotoCoreError):
        app.logger.exception("Query /api/papers/author failed")
        return json_error(500, "Server error")

    papers = [normalize_paper_summary(item) for item in items]
    return jsonify({
        "author": author_name,
        "papers": papers,
        "count": len(papers),
    })


@app.route("/api/papers/keyword/<path:keyword>", methods=["GET"])
@require_auth
def api_papers_keyword(keyword):
    limit_raw = request.args.get("limit", "20")

    try:
        limit = int(limit_raw)
        if limit <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return json_error(400, "Missing or invalid parameters")

    try:
        items = get_items_by_keyword(keyword, limit=limit)
    except (ClientError, BotoCoreError):
        app.logger.exception("Query /api/papers/keyword failed")
        return json_error(500, "Server error")

    papers = [normalize_paper_summary(item) for item in items]
    return jsonify({
        "keyword": keyword,
        "papers": papers,
        "count": len(papers),
    })


@app.route("/api/papers/<string:arxiv_id>/arxiv", methods=["GET"])
def api_arxiv_redirect(arxiv_id):
    try:
        item = get_item_by_arxiv_id(arxiv_id)
    except (ClientError, BotoCoreError):
        app.logger.exception("Query /api/papers/<arxiv_id>/arxiv failed")
        return json_error(500, "Server error")

    if not item:
        return json_error(404, "Paper not found", arxiv_id=arxiv_id)

    response = make_response("", 302)
    response.headers["Location"] = f"https://arxiv.org/abs/{arxiv_id}"
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
