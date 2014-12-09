import sys,os
import pickle
import ScanPlate
import GetLatLon
import tempfile
import PIL
import pika
import redis

# Modified to report the image contacts to the REDIS database

def imageType(filename):
    try:
        i=PIL.Image.open(filename)
        return i.format
    except IOError:
        return False

hostname= os.environ['RABBIT_HOST'] if 'RABBIT_HOST' in os.environ else 'rabbitmq-server.local'

def photoInfo(pickled):
    #
    # Add redis key/value for redisByChecksum and redisByName
    # 

    redisByChecksum = redis.Redis(host='redis-server.local', db=1)
    redisByName = redis.Redis(host='redis-server.local', db=2)
    redisMD5ByLicense = redis.Redis(host='redis-server.local', db=3)
    redisNameByLicense = redis.Redis(host='redis-server.local', db=4)

    print "pickled item is ", len(pickled),"bytes"
    unpickled = pickle.loads(pickled)
    print "File name was", unpickled[0], "digest is ", unpickled[1]
    photoFile,photoName = tempfile.mkstemp("photo")
    os.write(photoFile, unpickled[2])
    os.close(photoFile)
    newPhotoName = photoName + '.' + imageType(photoName)
    os.rename(photoName, newPhotoName)
    print "Wrote it to ", newPhotoName
    print "License:", ScanPlate.getLikelyLicense( newPhotoName )
    print "GeoTag:", GetLatLon.getLatLon( newPhotoName )
    # os.remove(newPhotoName)

    scanList = ScanPlate.getLikelyLicense( newPhotoName )

    # Check redis checksum 
    checksumKey = str(unpickled[1])
    if redisByChecksum.exists(checksumKey):
        print "Existing Redis checksum entry at key: '%s' " % (checksumKey)
    else:
        # scanList = ScanPlate.getLikelyLicense( newPhotoName )
        for ii in reversed(scanList):
            redisByChecksum.lpush(checksumKey, ii[0])
            print "Added (key, value) pair (%s, %s)" % (checksumKey, ii[0])

    # Check redis by file name
    checkName = str(newPhotoName)
    if redisByName.exists(checkName):
        print "Existing Redis name entry at key: '%s' " % (checkName)
    else:
        # scanList = ScanPlate.getLikelyLicense( newPhotoName )
        for ii in reversed(scanList):
            redisByName.lpush(checkName, ii[0])
            print "Added (key, value) pair (%s, %s)" % (checkName, ii[0])

    # Check MD5 By License
    for ii in scanList:

        checksumValue = str(unpickled[1])
        if redisMD5ByLicense.exists(ii[0]):
            print "Existing MD5ByLicense entry at key: '%s' " % (ii[0])
        else:
            redisMD5ByLicense.lpush(ii[0], checksumValue)
            print "Added (key, value) pair (%s, %s)" % (checksumValue, ii[0])

    # Check Name By License
    for ii in scanList:

        checkNameValue = str(newPhotoName)
        if redisMD5ByLicense.exists(ii[0]):
            print "Existing NameByLicense entry at key: '%s' " % (ii[0])
        else:
            redisMD5ByLicense.lpush(ii[0], checkNameValue)
            print "Added (key, value) pair (%s, %s)" % (checkNameValue, ii[0])

    os.remove(newPhotoName)

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=hostname))
channel = connection.channel()

channel.exchange_declare(exchange='scanners',type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='scanners',queue=queue_name)

print ' [*] Waiting for logs. To exit press CTRL+C'

def callback(ch, method, properties, body):
    photoInfo(body)

channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()
