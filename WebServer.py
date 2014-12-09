import os

#from gevent import monkey
#monkey.patch_all()
from flask import Flask
#from gevent import wsgi

from flask import Flask, request, redirect, url_for
from werkzeug import secure_filename
import pickle
import PIL
import pika
import hashlib

# Should accept photos and answer the '/check-by-...' REST calls documented at bottom

hostname= os.environ['RABBIT_HOST'] \
          if 'RABBIT_HOST' in os.environ else 'rabbitmq-server.local'
connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname))
channel = connection.channel()


app = Flask(__name__)

UPLOAD_FOLDER = '/tmp'
ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route("/")
def hello():
    return "Hello World!"

##
## This web service accepts an image file
##  
@app.route("/scan", methods=['POST', 'GET'])
def scan():
    if request.method == 'POST':
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            sFileName =  os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save( sFileName )
            print "File is ", sFileName
            fd = open(sFileName, 'rb')
            fileContents = fd.read()
            #
            # Prepare a tuple of the file name and file contents
            #
            digest = hashlib.md5(fileContents).hexdigest()
            tup = (sFileName, digest, fileContents)
            pickled = pickle.dumps(tup)
            #
            # You can print it out, but it is very long
            #
            print "pickled item is ", len(pickled),"bytes"
            #
            # Send the picture to the scanners
            #
            channel.exchange_declare(exchange='scanners',type='fanout')
            channel.basic_publish(exchange='scanners',
                                  routing_key='',
                                  body=pickled)
            print " [x] Sent photo ", sFileName
            os.remove(sFileName)
            return '{"digest":"%s"}' % (digest)
        else:
            abort(403)


@app.route("/check-by-md5/<checksum>")
def check_by_md5(checksum):

    redisByChecksum = redis.Redis(host='redis-server.local', db=1)

    key = str(checksum)
    resultList = []

    for ii in range(redisByChecksum.llen(key)):
        resultList.append( redisByChecksum.lindex(key, ii) )
    print "Checksum is ", resultList
        
@app.route("/check-by-name/<filename>")
def check_by_name(filename):

    redisByName = redis.Redis(host='redis-server.local', db=2)

    key = str(filename)
    resultList = []

    for ii in range(redisByName.llen(key)):
        resultList.append( redisByName.lindex(key, ii) )
    print "Filename is ", resultList
        

@app.route("/name-by-license/<license>")
def name-by-license(license):

    redisNameByLicense = redis.Redis(host='redis-server.local', db=4)

    key = str(license)
    resultList = []

    for ii in range(redisNameByLicense.llen(key)):
        resultList.append( redisNameByLicense.lindex(key, ii) )
    print "License is ", resultList


@app.route("/md5-by-licenses/<license>")
def md5-by-licenses(license):

    redisMD5ByLicense = redis.Redis(host='redis-server.local', db=3)

    key = str(license)
    resultList = []

    for ii in range(redisMD5ByLicense.llen(key)):
        resultList.append( redisMD5ByLicense.lindex(key, ii) )
    print "License is ", resultList

#server = wsgi.WSGIServer(('0.0.0.0', 8080), app)
#server.serve_forever()

if __name__ == "__main__":
    app.debug = True
    app.run(host='0.0.0.0', port=8080)
