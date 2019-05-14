import hashlib
import time
import urllib #for url encoding
import urllib.request
import base64
import sys

try:
    import json
except ImportError:
    import simplejson as json

try:
    import eventlet
    from eventlet.green import urllib as urllib2
except ImportError:
    print("You need to pip install eventlet. Quitting now...")
    sys.exit()
 

class Mixpanel(object):
 
    def __init__(self, api_key, api_secret, token):
        self.api_key = api_key
        self.api_secret = api_secret
        self.token = token
 
    def request(self, params, format = 'json'):
        '''let's craft the http request'''
        params['api_key']=self.api_key
        params['expire'] = int(time.time())+10000 # 600 is ten minutes from now
        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)
 
        request_url = 'http://mixpanel.com/api/2.0/engage/?' + self.unicode_urlencode(params)
 
        request = urllib.request.urlopen(request_url)
        data = request.read()
        return data
 
    def hash_args(self, args, secret=None):
        '''Hash dem arguments in the proper way
        join keys - values and append a secret -> md5 it'''
 
        for a in args:
            if isinstance(args[a], list): args[a] = json.dumps(args[a])
 
        args_joined = ''
        for a in sorted(args.keys()):
            print(a)
            print(args[a])
            if isinstance(a, str):
                args_joined += a
            else:
                args_joined += str(a)
 
            args_joined += "="
 
            if isinstance(args[a], str):
                args_joined += args[a]
            else:
                args_joined += str(args[a])
 
        hash = hashlib.md5(args_joined.encode('utf-8'))
 
        if secret:
            hash.update(secret.encode('utf-8'))
        elif self.api_secret:
            hash.update(self.api_secret.encode('utf-8'))
        return hash.hexdigest()
 
    def unicode_urlencode(self, params):
        ''' Convert stuff to json format and correctly handle unicode url parameters'''
 
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)
 
        result = urllib.parse.urlencode([(k, isinstance(v, str) and v.encode('utf-8') or v) for k, v in params])
        return result
 
    def update(self, userlist, uparams):
        url = "http://api.mixpanel.com/engage/"
        batch = []
        for user in userlist:
            distinctid = user['$distinct_id']
            tempparams = {
                    '$token':self.token,
                    '$distinct_id':distinctid,
                    '$ignore_alias':True
                    }
            tempparams.update(uparams)
            batch.append(tempparams)
 
        payload = {"data":base64.b64encode(json.dumps(batch).encode('utf-8')), "verbose":1,"api_key":self.api_key}
 
        #response = urllib.request.urlopen(url, urllib.parse.urlencode(payload).encode("utf-8"))
        print("1")
        data = urllib.parse.urlencode(payload)
        print("2")
        data = data.encode('utf-8')
        print("3")
        req = urllib.request.Request(url)
        print("4")
        response = urllib.request.urlopen(req, data=data)
        print("5")
        message = response.read()
 
        '''if something goes wrong, this will say what'''
        if json.loads(message.decode('utf-8'))['status'] != 1:
            print(message)
 
    def batch_update(self, users, params):
        pool = eventlet.GreenPool(size=10) # increase the pool size if you have more memory (e.g., a server)
        while len(users):
            batch = users[:50]
            pool.spawn(self.update, batch, params)
            users = users[50:]
        pool.waitall()
        print("Done!")
 
def deleteUsers(project):
    parameters = {}
    # this is the filter to delete people who have not log in in the last 90 days
    parameters.update({
        'selector':'(datetime(' + str(int(time.time())) + '- 26352000) > properties["$last_seen"])'})
    print('here')
    response = project.request(parameters)
    print('there')
    parameters.update({
                'session_id' : json.loads(response.decode('utf-8'))['session_id'],
                'page':0
                })
    print('try')
    global_total = json.loads(response.decode('utf-8'))['total']
 
    print("Here are the # of people %d" % global_total)
    fname = project.token + "-" + str(int(time.time())) + ".txt"
    has_results = True
    total = 0
    print(fname)
    f = open(fname, 'w')
    while has_results:
        print('maybe')
        responser = json.loads(response.decode('utf-8'))['results']
        total += len(responser)
        has_results = len(responser) == 1000
        for data in responser:
                f.write(json.dumps(data)+'\n')
        print("%d / %d" % (total,global_total))
        project.batch_update(responser, {'$delete':''})
        parameters['page'] += 1
        if has_results:
            response = project.request(parameters)
 
if __name__ == '__main__':
    projList =[
        # project 1
        Mixpanel(
            api_key='',
            api_secret='',
            token=''
        ),
        # project 2
        Mixpanel(
            api_key='',
            api_secret='',
            token=''
        ),
    ]
 
    for project in projList:
        deleteUsers(project)