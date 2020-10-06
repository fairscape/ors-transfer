#© 2020 By The Rector And Visitors Of The University Of Virginia

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import requests
import flask
import json
import os
from functools import wraps

AUTH_SERVICE = os.environ.get("AUTH_SERVICE", "http://clarklab.uvarc.io/auth")
ISSUER = "ors:transfer"


def token_required(handler):
    '''
    Function Wrapper for all endpoints that checks that an Authorization is present in request headers.
    If not the wrapper will return an error.

    Used for API service calls where a Globus Token is required.
    '''

    @wraps(handler)
    def wrapped_handler(*args, **kwargs):
        if flask.request.headers.get("Authorization") is None:
            return flask.Response(
                response= json.dumps({"error": "Request Missing Authorization Header"}),
                status=403,
                content_type="application/json"
            )

        token_response = requests.post(
            url = AUTH_SERVICE + "/inspect",
            headers = {"Authorization": flask.request.headers.get("Authorization")}
            )

        if token_response.status_code == 204:
            return handler(*args, **kwargs)
        else:
            return flask.Response(
                    response=json.dumps({"error": "failed to authorize user"}),
                    status=401,
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
        if flask.request.headers.get("Authorization") is not None:
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


def register_resource(user_token, resource):
    '''
    Post a record of a created object in the Auth service
    '''

    resp = requests.post(
        url = AUTH_SERVICE + "resource",
        data = json.dumps({
            "@id": resource,
            "owner": user_token
            })
        )

    if resp.status_code == 200:
        return True

    else:
        return False


def delete_resource(user_token, resource):

    resp = requests.delete(
        url = AUTH_SERVICE + "resource/" + resource,
        headers = {"Authorization": "Bearer {user_token}"}
        )

    if resp.statuse_code != 200:
        return False

    return True


def create_policy(user_token, resource, principal, action, allow):
    '''
    Used to change set permissions on objects from this service at the centrilized auth service
    '''

    policy_body = {
        "resouce": resource,
        "principal": principal,
        "action": action,
        "allow": allow,
        "issuer": ISSUER
    }


    policy_response = requests.post(
        url = AUTH_SERVICE + "policy",
        data=json.dumps(policy_body),
        headers = {"Authorization": "Bearer {user_token}"}
        )


    if policy_response.status_code == 200:
        return True

    # FIXME: handle different errors and return

    else:
        return False
