#!/usr/bin/env python

__author__ = 'Phillip Watts'

import os, sys, time
import types
from subprocess import Popen, PIPE

import boto
from boto.s3.connection import S3Connection
from boto.ec2.connection import EC2Connection
from boto.sqs.connection import SQSConnection
from boto.s3.key import Key
from boto.sqs.message import Message

SSH = 22

class BotoAWS(object):
    """ A class to experiment with AWS
        functionality using python boto

        S3,  EC2,  SQS        

        TODO:  this needs a logging method
                will use print during debug
        NOTE: To port this to Open Stack Nova API
                will need to incorporate tokens.
                Research.
    """
    def __init__(self,accessFile='./access'):
        """ default access(settings) file is 
              'access' in current directory
        """
        self.__access = {
            'login': '',    'pwd'  : '',
            'access_key': '', 'secret_key': '',
            'aws_acct_id': '', 'canonical_user_id': ''
                        }
        self.__loadAccess(accessFile)

    def __loadAccess(self,accessFile): 
        """ Build the access dict from a text 
             file of values that match the dict
        """ 
        try:
            f = open(accessFile,'r')
            flines = f.readlines()
            f.close()
        except: 
            print 'No Access File'
            return 
        # access file is lines of name/key pairs
        for line in flines:
            line = line.strip()
            if not line:        continue  # allow blank lines
            if line[0] == '#':  continue  # allow comments
            wds = line.split()
            if len(wds) != 2: continue
            self.__access[wds[0]] = wds[1]

    def makeConn(self,connType):
        try:
            self.__conn = connType(self.__access['access_key'], 
                            self.__access['secret_key'])
            return self.__conn
        except:
            print 'Connection Failed'
            return None
        # How do you know if the connection was good?
        # TODO: dig deeper into the conn class

    def getLogin(self):
        return self.__access['login']

    def close(self):
        self.__conn.close()

class BotoS3(BotoAWS):
    def __init__(self):
        BotoAWS.__init__(self,accessFile='./access')
        self.__buckets = {}

    def makeS3Conn(self):
        self.__conn = self.makeConn(S3Connection)
        return self.__conn

    def fileToBucket(self, fileName, bucket, toKey=None):
        """ bucket is an overload.
              if its a string use makeBucketName to
                find it in __buckets.
              otherwise assume it is a bucket object
        """
        if not os.path.exists(fileName):
            print 'File Not Found'
            return -1
        bucket = self.resolveBucket(bucket)
        if bucket ==  -1: return -1

      # Now write file to bucket
        if not toKey: toKey = fileName
        key = Key(bucket)
        key.key = toKey
        key.set_contents_from_filename(fileName)

    def bucketToFile(self, bucket, fileName, saveAs=None):
        """ bucket is an overload.
              if its a string use makeBucketName to
                find it in buckets.
              otherwise assume it is a bucket object
        """
        bucket = self.resolveBucket(bucket)
        if bucket ==  -1: return -1

      # Now read file from bucket
        if not saveAs: saveAs = fileName
        key = Key(bucket)
        key.key = fileName
        key.get_contents_to_filename(saveAs)

    def getBucketList(self):
        """ Returns list of tuples:
              (name, bucket)
        """
        bucketList = self.__conn.get_all_buckets()
        for bucket in bucketList:
            self.__buckets[ str(bucket.name) ] = bucket
        return self.__buckets.items()

    def getKeyList(self,bucket):
        bucket = self.resolveBucket(bucket)
        kl = bucket.get_all_keys()
        return kl

    def resolveBucket(self,bucket):        
        if type(bucket) is types.StringType:
            bucketName = self.makeBucketName(bucket)
            try:    bucket = self.__buckets[bucketName]
            except:
                print 'Bucket does not exist in this instance'
                return -1
        return bucket

    def makeBucket(self, bucketName, rawName=False):
        """ If the bucket already exists, this
              just gets an instance
            You can pass a rawname if confident
              it is unique
        """
        if not rawName:
            bucketName = self.makeBucketName(bucketName)
        try:
            bucket = self.__conn.create_bucket(bucketName)
        except:
            print 'Create Bucket Failed'
            # TODO: handle exception better

       # return the bucket to the user AND
       #  register the bucket by name
        self.__buckets[bucketName] = bucket
        return bucket
        
    def makeBucketName(self,bucketName):
        """ Bucket name is email login with
            @ -> _ + _bucket

            TODO: this is ok for testing but
              fix later to comply w DNS form.
        """
        login = self.getLogin().replace('@','_')
        bucketName = login + '_' + bucketName
        return bucketName

class BotoEC2(BotoAWS):
    def __init__(self):
        BotoAWS.__init__(self,accessFile='./access')

    def makeEC2Conn(self):
        self.__conn = self.makeConn(EC2Connection)
        return self.__conn

    def getImageList(self):
        return  self.__conn.get_all_images()
    
    def installImage(self,image,keyName=''):
        # At this time I do not grok why conn is not
        #  involved in starting an image.
        #  Amazon is relying on my IP address?
        #   My http connection?
        #   That does not seem secure
        #   Or is this a boto thing? research.


        # for sake of cgi, create a method
        #  for these print statements later
        print
        print '--- Installing ---'
        myos = image.run(key_name=keyName)
        beg = time.time()
        print myos.instances
        inst = myos.instances[0]
        print inst.state
        while True:
            time.sleep(5)
            inst.update()
            print inst.state
            if str(inst.state) == 'running': break
        elap = time.time() - beg
        print
        print 'Provisioning took %d seconds' % int(elap)
        dn = str(inst.public_dns_name)
        print 'DOMAIN OF THIS =',dn 
        print
        return inst, dn

    def termImage(self,inst):
        print '--- Terminating ---'
        inst.stop()
        beg = time.time()
        print inst.state
        while True:
            time.sleep(5)
            inst.update()
            print inst.state
            if str(inst.state) == 'stopped': break
        inst.terminate()
        elap = time.time() - beg
        print
        print 'Termination took %d seconds' % int(elap)

    def sshAccess(self):
        defgroup = self.__conn.get_all_security_groups(['default'])[0]
        print '--rules before'
        print defgroup.rules
        print 
        try:
            defgroup.authorize(ip_protocol='tcp', 
                    from_port=SSH, to_port=SSH, cidr_ip='0.0.0.0/0')
            # throws exception if rule already exists
            # need to parse and handle
            # a rule appears to be permanent and across
            #    all instances. ??? more research
        except: pass
        print '--rules after'
        print defgroup.rules
        print

class BotoSQS(BotoAWS):
    def __init__(self):
        BotoAWS.__init__(self,accessFile='./access')
        self.__qByName = {}

    def makeSQSConn(self):
        self.__conn = self.makeConn(SQSConnection)

      # record existing queue names
        if self.__conn != None:
            qList = self.__conn.get_all_queues()
            for q in qList:
                self.__qByName[q.name] = q  
        return self.__conn

    def createQueue(self,name,timeout=None):
        if timeout == None: timeout = 120
        try:    q = self.__qByName[name]
        except: 
           try:
                q = self.__conn.create_queue(name,timeout)
                self.__qByName[name] = q
           except:
                print 'Queue Create Failed'
                return None
        return q

    def queuePut(self,name,msg='',timeout=None):

      # if queue already exists, this will get the object
        q = self.createQueue(name,timeout=timeout)
      ###

        m = Message()
        m.set_body(msg)
        ret = q.write(m)
        if type(ret) is not types.InstanceType:
             print 'FAILED'

    def queueGet(self,name):
        q = self.__qByName[name]

        msg1 = q.read()
        if msg1 is not None:
            msg =  msg1.get_body()
            q.delete_message( msg1 )
            return msg
        else: return None

    def queueList(self):
        return self.__qByName.items()

if __name__ == '__main__':
    argl = sys.argv

# Write a pyunit based test later
#  This will do for proof of concept

    if 's3' in argl:
        print 'S3 TESTING'
        bos3 = BotoS3()
        bos3.makeS3Conn()
        bucket = bos3.makeBucket('phil101')
        ret = bos3.fileToBucket('tmplmain','phil101')
            # OR: do it this way
            # bos3.fileToBucket('tmplmain',bucket)
        ret = bos3.bucketToFile('phil101','tmplmain',saveAs='tmplmain3')
            # OR: do it this way
            # ret = bos3.bucketToFile(bucket,'tmplmain',
            #                         saveAs='tmplmain3')
        print
        bucketList = bos3.getBucketList()
        for bucket in bucketList:
            bucketName = bucket[0]
            bucketObj  = bucket[1]
            print '---------------------------'
            print bucketName
            keyList = bos3.getKeyList(bucket[1])
            for key in keyList:
                print '-- ', key.name
        bos3.close()
     
    if 'ec2' in argl:
        print 'EC2 TESTING'
        bos2 = BotoEC2()
        conn = bos2.makeEC2Conn()
        # conn.get_all_keypairs()
        # sys.exit()
        imageList = bos2.getImageList()
        # for image in imageList:
        for i in range( len(imageList) ):
            if 'ubuntu-12.04' in imageList[i].location.lower():
              if 'lampstack' in imageList[i].location.lower():
                if '386-ebs' in imageList[i].location.lower():
                    print  i, '=', imageList[i].location 
        print
        ind = raw_input('Which instance index? ')
        ind = int(ind)
        image =  imageList[ind]
        print
        print  image.location 
        print 
        a = raw_input('Run This? (y/n) ')
        if a.lower() == 'y':
            inst, dn = bos2.installImage(image,keyName='sshaccess')
        print
        print 'Allow it 30 secs to show up on EC2 Dashboard'
        print
        while True:
            a = raw_input('Terminate this Instance? (y/n) ')
            if a.lower() == 'y':
                bos2.termImage(inst)
                break

            raw_input('Ok then, press enter to test file transfer ')
            bos2.sshAccess()
            time.sleep(3)
            cmd = 'scp -q -i ./sshaccess.pem cs-devguide-20120921.pdf bitnami@%s:a.pdf' % dn
            print
            print cmd
            args = cmd.split()
            p = Popen(args)
            time.sleep(3)

            cmd = 'scp -q -i ./sshaccess.pem  bitnami@%s:a.pdf x.pdf' % dn
            print
            print cmd
            args = cmd.split()
            p = Popen(args)
            time.sleep(3)

            raw_input('Press Enter to continue. ')
            #makeAccess()            
        bos2.close()

    if 'sqs' in argl:
        print 'SQS TESTING'
        bosq = BotoSQS()
        conn = bosq.makeSQSConn()
        if conn != None: 
            bosq.queuePut('q1',
              '%s HELLO FROM Q1' % time.strftime("%Y/%m/%d %H:%M:%S"))
            time.sleep(1)
            bosq.queuePut('q1',
              '%s HELLO FROM Q1' % time.strftime("%Y/%m/%d %H:%M:%S"))
            time.sleep(1)
            bosq.queuePut('q2',
              '%s HELLO FROM Q2' % time.strftime("%Y/%m/%d %H:%M:%S"))
            time.sleep(1)
            bosq.queuePut('q2',
              '%s HELLO FROM Q2' % time.strftime("%Y/%m/%d %H:%M:%S"))
            #time.sleep(2)
            print
            msg = 1
            while msg:
                msg = bosq.queueGet('q1')
                print msg
            print
            msg = 1
            while msg:
                msg = bosq.queueGet('q2')
                print msg
            msg = 1
            while msg:
                msg = bosq.queueGet('q5')
                print msg
            bosq.close()
    
# TO DOs:
#  logging of course
#  an html/js method(s)
#  much better exception handling
#  AWS RDS
