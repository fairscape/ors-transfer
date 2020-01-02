import requests
import flask
import json

AUTH_SERVICE = "http://ors-auth.uvadcos.io/"
ISSUER = "ors:transfer"


def token_required(handler):
    '''
    Function Wrapper for all endpoints that checks that an Authorization is present in request headers
    '''

    def wrapped_handler(request):
        if request.headers.get("Authorization") is not None:
            return handler

        else:
            return flask.Response(
                repsonse={"error": "Request Missing Authorization Header"},
                status=403,
                content_type="application/json"
            )

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
