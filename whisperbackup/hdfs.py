#!/usr/bin/env python
#
#
#   Alexander Svyatov: Alexander Svyatov <asvyatov88@gmail.com>
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import __main__
import glob
import logging
import os
import pyhdfs
import tarfile 

logger = logging.getLogger(__main__.__name__)

class hdfs(object):

    def __init__(self, bucket, webhdfs_hosts_list, noop=False):
        self.bucket = bucket
        self.noop = noop
        self.client = pyhdfs.HdfsClient(hosts=webhdfs_hosts_list)
    
    def list(self, prefix=""):
        """ Return all keys in this bucket."""
        try:
            list_rep = self.client.listdir(self.bucket + "/" + prefix)
            for i in list_rep:
                # Remove preceding bucket name and potential leading slash from returned key value
                i = i.replace(self.bucket, "").replace('tar', 'wsp.sz')
                if i[0] == '/': i = i[1:]
                yield i
        except pyhdfs.HdfsFileNotFoundException:
            pass
        
    def get(self, src):
        """Return the contents of src from hdfs as a string."""
        tarName = "%s/%s.tar" % (self.bucket, src)
        if not self.client.exists(os.path.dirname(tarName)):
            return None
        # k = ""
        try:
            # with self.client.open(self.bucket + "/" + src) as f:
            #     k = f.read()
            # copy_to_local(src: str, localdest: str, **kwargs)
            if not os.path.exists(os.path.dirname(tarName)):
                os.makedirs(os.path.dirname(tarName))
            self.client.copy_to_local(tarName, tarName)
            self.extractTar(os.path.dirname(tarName), tarName)
        except Exception as e:
            logger.info("Exception during get: %s" % str(e))
        # return k

    def put(self, dst, timestamp):
        """Store the contents of the string data at a key named by dst
           on hdfs."""

        if self.noop:
            logger.debug("No-Op Put: %s" % dst)
        else:
            filename = "%s/%s%s.tar" % (self.bucket, dst, timestamp)
            self.populateTar(os.path.dirname(filename), filename)
            if not self.client.exists(os.path.dirname(filename)):
                self.client.mkdirs(os.path.dirname(filename))
            try:
                self.client.copy_from_local(filename, filename, overwrite = True)
            except Exception as e:
                logger.warning("Exception during put: %s" % str(e))

    def populateTar(self, dst, tarName):
        os.chdir(dst)
        with tarfile.open(tarName, "w") as tarHandler:
            tarHandler.add(".")
            tarHandler.close()

    def extractTar(self, dst, tarName):
        os.chdir(dst)
        with tarfile.open(tarName, 'r:') as tarHandler:
            tarHandler.extractall()
            tarHandler.close()
        # os.remove(tarName)

    def delete(self, src):
        """Delete the object on hdfs referenced by the key name src."""

        if self.noop:
            logger.info("No-Op Delete: %s.tar" % self.bucket + src)
        else:
            logger.info("Trying to delete %s.tar" % self.bucket + src)
            self.client.delete(self.bucket + src + ".tar")
