import requests
import flask
import json
import os
from functools import wraps

AUTH_SERVICE = os.environ.get("AUTH_SERVICE", "http://auth.uvadcos.io/")
ISSUER = "ors:transfer"


def token_required(handler):
    '''
    Function Wrapper for all endpoints that checks that an Authorization is present in request headers.
    If not the wrapper will return an error.

    Used for API service calls where a Globus Token is required.
    '''

    @wraps(handler)
    def wrapped_handler(*args, **kwargs):
        if request.headers.get("Authorization") is not None:
            return handler(*args, **kwargs)

        else:
            return flask.Response(
                repsonse={"error": "Request Missing Authorization Header"},
                status=403,
                content_type="application/json"
            )

    return wrapped_handler


def token_redirect(handler):
    '''
    Function Wrapper for all endpoints that checks for an Authorization token in request headers, if not
    the wrapper will redirect the user to login.

    Used for frontend views where a user must be logged in to use some part of the page.
    i.e. deleting a identifier from landing page interface
    '''

    @wraps(handler)
    def wrapped_handler(*args, **kwargs):
        if request.headers.get("Authorization") is not None:
            return handler(*args, **kwargs)
        else:
            return flask.redirect(AUTH_SERVICE + "login")

    return wrapped_handler


def check_permission(user_token, resource, action):
    '''
    Issues a permissions challenge to the token for the request
    '''

    challenge_body = {
        "principal": user_token,
        "resource": resource,
        "action": action,
        "issuer": ISSUER
    }

    challenge_response = requests.post(
        AUTH_SERVICE + "challenge",
        data=json.dumps(challenge_body)
    )

    if challenge_response.status_code == 200:
        return True

    else:
        return False


def register_resource(identifier, owner):
    '''
    Post a record of a created object in the Auth service
    '''

    resource = {
        "@id": identifier,
        "owner": owner
    }

    resp = requests.post(AUTH_SERVICE + "resource", data=json.dumps(resource))

    if resp.status_code == 200:
        return True

    else:
        return False
