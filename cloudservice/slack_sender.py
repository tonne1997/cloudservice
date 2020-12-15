from typing import List
import os
import socket
import functools
import requests
import datetime
import json
import subprocess
import traceback

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
def post_slack(webhook_url, data, env = 'kubeflow'):
    if env == 'kubeflow':
        process = subprocess.Popen(['gcloud', 'auth', 'print-identity-token'], stdout=subprocess.PIPE)
        token = process.communicate()[0].decode(encoding='utf-8')
        headers = {
            'Authorization' : 'bearer ' + token.strip(),
            'Content-type': 'application/json',
        }
        cloud_function_url = 'https://asia-east2-vinid-data-science-prod.cloudfunctions.net/slack-noti-kubeflow-hk'
        requests.post(cloud_function_url, headers = headers, data = data['text'])
    else:
        requests.post(webhook_url, data)



def slack_sender(webhook_url: str, channel: str = "", user_mentions: List[str] = [], env = 'kubeflow'):
    dump = {
        "username": "Knock Knock",
        "channel": channel,
        "icon_emoji": ":clapper:",
    }
    def decorator_sender(func):
        @functools.wraps(func)
        def wrapper_sender(*args, **kwargs):
            start_time = datetime.datetime.now()
            host_name = socket.gethostname()
            contents = [
                'Machine name: {}'.format(host_name),
                'Starting date: *{}*'.format(start_time.strftime(DATE_FORMAT))
            ]
            contents.append(' '.join(user_mentions))
            dump['text'] = '\n'.join(contents)
            dump['icon_emoji'] = ':clapper:'
            post_slack(webhook_url, json.dumps(dump), env = env)
            try:
                value = func(*args, **kwargs)
                end_time = datetime.datetime.now()
                elapsed_time = end_time - start_time
                contents = ['End date: *{}*'.format(end_time.strftime(DATE_FORMAT)),
                            'Duration: *{}*'.format(str(elapsed_time))
                           ]
                try:
                    str_value = str(value)
                    contents.append('Contents: {}'.format(str_value))
                except:
                    contents.append('Contents: {}'.format("ERROR - Couldn't str the returned value."))

                contents.append(' '.join(user_mentions))
                dump['text'] = '\n'.join(contents)
                dump['icon_emoji'] = ':clapper:'
                post_slack(webhook_url, json.dumps(dump), env = env)
                return value
            except Exception as ex:
                end_time = datetime.datetime.now()
                elapsed_time = end_time - start_time
                contents = ["Your training has crashed ☠️",
                            'Starting date: *{}*'.format(start_time.strftime(DATE_FORMAT)),
                            'Crash date: {}'.format(end_time.strftime(DATE_FORMAT)),
                            'Crashed training duration: {}\n\n'.format(str(elapsed_time)),
                            "Here's the error:",
                            '{}\n\n'.format(ex),
                            "Traceback:",
                            '{}'.format(traceback.format_exc())
                           ]
                contents.append(' '.join(user_mentions))
                dump['text'] = '\n'.join(contents)
                dump['icon_emoji'] = ':skull_and_crossbones:'
                post_slack(webhook_url, json.dumps(dump), env = env)
                raise ex
        return wrapper_sender
    return decorator_sender
