import json
import os
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# ========== ENVIRONMENT VARIABLES ==========
# (configured in Lambda console)
LEX_BOT_ID = os.environ["LEX_BOT_ID"]
LEX_BOT_ALIAS_ID = os.environ["LEX_BOT_ALIAS_ID"]
LEX_LOCALE_ID = os.environ.get("LEX_LOCALE_ID", "en_US")

ES_ENDPOINT = os.environ["ES_ENDPOINT"]          # e.g. search-photos-xxxx.us-east-1.es.amazonaws.com
ES_INDEX = os.environ.get("ES_INDEX", "photos")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# ========== CLIENTS ==========
lex = boto3.client("lexv2-runtime")

session = boto3.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    AWS_REGION,
    "es",  # service name for OpenSearch/ES domain
    session_token=credentials.token,
)

es = OpenSearch(
    hosts=[{"host": ES_ENDPOINT, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)


# ========== HELPER FUNCTIONS ==========

def get_query_from_event(event):
    """
    Support both:
      { "q": "show me dog" }                        (console test)
      { "queryStringParameters": { "q": "..." } }   (API Gateway)
    """
    if "q" in event:
        return event["q"]

    qsp = event.get("queryStringParameters") or {}
    return qsp.get("q")


def get_keywords_from_lex(q: str):
    """
    Call Amazon Lex V2 with text query q and return a list of keywords (slot values).
    """
    resp = lex.recognize_text(
        botId=LEX_BOT_ID,
        botAliasId=LEX_BOT_ALIAS_ID,
        localeId=LEX_LOCALE_ID,
        sessionId="lf2-session",
        text=q,
    )

    # slots live under sessionState.intent.slots
    slots = (
        resp.get("sessionState", {})
            .get("intent", {})
            .get("slots", {})
        or {}
    )

    keywords = []
    for slot in slots.values():
        if slot and "value" in slot:
            iv = slot["value"].get("interpretedValue")
            if iv:
                keywords.append(iv)

    return keywords


def search_photos_in_es(keywords):
    """
    Search the 'photos' index in OpenSearch using the keywords from Lex.
    Returns a list of result objects for the API response.
    """
    if not keywords:
        return []

    # require all keywords (AND). If you wanted OR, you'd use "should" instead.
    must_clauses = []
    for kw in keywords:
        must_clauses.append({
            "multi_match": {
                "query": kw,
                "fields": [
                    "labels",
                    "labels.keyword",
                    "objects",
                    "objects.keyword",
                ],
            }
        })

    query = {
        "query": {
            "bool": {
                "must": must_clauses
            }
        }
    }

    response = es.search(index=ES_INDEX, body=query)
    hits = response.get("hits", {}).get("hits", [])

    results = []
    for hit in hits:
        source = hit.get("_source", {})

        # Shape this to match your assignment's API spec.
        # This is the common schema used in the photo search lab:
        result_item = {
            "bucket": source.get("bucket"),
            "objectKey": source.get("objectKey"),
            "labels": source.get("labels", []),
        }
        results.append(result_item)

    return results


# ========== MAIN HANDLER ==========

def lambda_handler(event, context):
    # 1. Read query q
    q = get_query_from_event(event)
    print("DEBUG event:", json.dumps(event))
    print("DEBUG raw q:", q)

    if not q:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Missing query parameter 'q'"}),
        }

    # 2. Disambiguate using Lex (Part 2)
    keywords = get_keywords_from_lex(q)
    print("DEBUG keywords from Lex:", keywords)

    # 🔑 If Lex returns nothing, fall back to the raw query string
    if not keywords:
        keywords = [q]
        print("DEBUG falling back to raw query:", keywords)

    # 3. Search ES with whatever keywords we have
    es_results = search_photos_in_es(keywords)
    print("DEBUG ES results:", es_results)

    body = {"results": es_results}

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
