import os
import json
import uuid

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# --- Env vars ---
LEX_BOT_ID = os.environ["LEX_BOT_ID"]
LEX_BOT_ALIAS_ID = os.environ["LEX_BOT_ALIAS_ID"]
LEX_LOCALE_ID = os.environ.get("LEX_LOCALE_ID", "en_US")

ES_ENDPOINT = os.environ["ES_ENDPOINT"]      # without https://
ES_INDEX = os.environ.get("ES_INDEX", "photos")
S3_BUCKET = os.environ.get("S3_BUCKET")      # optional

region = os.environ.get("AWS_REGION", "us-east-1")

# --- Clients ---

lex_client = boto3.client("lexv2-runtime")

session = boto3.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    "es",
    session_token=credentials.token,
)

os_client = OpenSearch(
    hosts=[{"host": ES_ENDPOINT, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)


def get_keywords_from_lex(query: str):
    """
    Send the user text to Lex and pull out the 'keywords' slot.
    Supports both single-value and multiple-value slots.
    Returns a list of keywords (0, 1 or more).
    """
    session_id = str(uuid.uuid4())  # new session for every search

    resp = lex_client.recognize_text(
        botId=LEX_BOT_ID,
        botAliasId=LEX_BOT_ALIAS_ID,
        localeId=LEX_LOCALE_ID,
        sessionId=session_id,
        text=query,
    )

    # Try sessionState.intent.slots first
    slots = (
        resp.get("sessionState", {})
        .get("intent", {})
        .get("slots", {})
    ) or {}

    # Fallback: sometimes slots appear in interpretations[0].intent.slots
    if not slots:
        interpretations = resp.get("interpretations") or []
        if interpretations:
            slots = (
                interpretations[0]
                .get("intent", {})
                .get("slots", {})
            ) or {}

    kw_slot = slots.get("keywords")
    if not kw_slot:
        print("No 'keywords' slot found in Lex response:", json.dumps(resp))
        return []

    keywords = []

    # --- Case 1: multi-value slot (allowMultipleValues = true) ---
    if "values" in kw_slot:
        for item in kw_slot.get("values") or []:
            v = (
                item.get("value", {})
                .get("interpretedValue")
            )
            if v:
                keywords.append(v.strip())

    # --- Case 2: classic single-value slot ---
    elif "value" in kw_slot:
        raw = kw_slot["value"]["interpretedValue"]  # e.g. "dog and cat"
        tmp = raw.replace(" and ", ",")
        parts = [p.strip() for p in tmp.split(",")]
        keywords.extend([p for p in parts if p])

    # Remove duplicates / empties
    keywords = [k for k in dict.fromkeys(keywords) if k]

    print("Lex keywords slot parsed as:", keywords)
    return keywords


def search_photos_in_opensearch(keywords):
    """
    Perform a search on the OpenSearch 'photos' index using the labels field.

    For multiple keywords (e.g. ["cat", "dog"]), we want OR semantics:
    return any photo that has at least one of the labels.
    """
    if not keywords:
        return []

    # OR over all keywords: a photo that matches ANY of them should be returned
    should_clauses = [{"match": {"labels": kw}} for kw in keywords]

    query = {
        "size": 50,
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1,
            }
        }
    }

    print("OpenSearch query:", json.dumps(query))

    resp = os_client.search(index=ES_INDEX, body=query)

    hits = resp.get("hits", {}).get("hits", [])
    results = []

    for hit in hits:
        src = hit.get("_source", {})

        # Optionally construct a URL to the S3 object
        url = None
        if S3_BUCKET and src.get("objectKey"):
            # adjust if you use CloudFront or a different URL pattern
            url = f"https://{S3_BUCKET}.s3.amazonaws.com/{src['objectKey']}"

        results.append(
            {
                "objectKey": src.get("objectKey"),
                "bucket": src.get("bucket"),
                "createdTimestamp": src.get("createdTimestamp"),
                "labels": src.get("labels", []),
                "url": url,
            }
        )

    return results


def lambda_handler(event, context):
    """
    Supports:
      - Direct invocation: { "q": "show me dog" }
      - API Gateway GET: event["queryStringParameters"]["q"]
      - API Gateway POST JSON body: { "q": "show me dog" }
    """
    print("Incoming event:", json.dumps(event))

    # 1. Extract query q
    q = None

    # a) Direct invocation or generic dict: { "q": "..." }
    if isinstance(event, dict):
        q = event.get("q")

    # b) From queryStringParameters: ?q=...
    if not q and isinstance(event, dict):
        qs = event.get("queryStringParameters") or {}
        q = qs.get("q")

    # c) From JSON body: { "q": "..." }
    if not q and isinstance(event, dict) and event.get("body"):
        try:
            body_json = event["body"]
            if isinstance(body_json, str):
                body_json = json.loads(body_json)
            if isinstance(body_json, dict):
                q = body_json.get("q")
        except Exception as e:
            print("Error parsing body JSON:", e)

    print("Raw query text:", q)

    if not q:
        # No query → empty results as per spec
        body = {"query": None, "keywords": [], "results": []}
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(body),
        }

    # 2. Get keywords from Lex
    try:
        keywords = get_keywords_from_lex(q)
    except Exception as e:
        print("Error calling Lex, falling back to simple split:", e)
        tmp = q.replace(" and ", ",")
        keywords = [p.strip() for p in tmp.split(",") if p.strip()]

    print("Final keywords used for search:", keywords)

    # 3. If no keywords → empty results
    if not keywords:
        body = {"query": q, "keywords": [], "results": []}
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(body),
        }

    # 4. Search OpenSearch
    results = search_photos_in_opensearch(keywords)

    # 5. Return results according to API spec
    body = {
        "query": q,
        "keywords": keywords,
        "results": results,
    }

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }
