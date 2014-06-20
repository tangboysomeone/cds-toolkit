# coding: utf-8
import paramiko
import datetime
import os
import argparse
#--no-create-info
DUMP_COMMAND = r"mysqldump -u root -pabcd1234 dumptest > dumptest.sql"
IMPORT_COMMAND = r"mysql -u root -pabcd1234 dumptest2 < dumptest.sql"

class RemoteShell:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def execute_cmd(self, cmd):
        try:
            self.sshClinet = paramiko.SSHClient()
            self.sshClinet.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.sshClinet.connect(self.host, self.port, self.user, self.password)
            stdin, stdout, stderr = self.sshClinet.exec_command(cmd)
            print stdout.readlines()
            print stderr.readlines()
        except Exception as e:
            print 'remote cmd execute failed', e
        finally:
            self.sshClinet.close()

    def download_file(self, filePath):
        try:
            t=paramiko.Transport((self.host,self.port))
            t.connect(username=self.user, password=self.password)
            sftp=paramiko.SFTPClient.from_transport(t)
            tmpFile = os.path.join(os.path.expanduser('~'), os.path.basename(filePath))
            print ''
            print '#########################################'
            print 'Beginning to download file  from %s to %s %s ' % (self.host, tmpFile, datetime.datetime.now())
            sftp.get(filePath, tmpFile)#下载
            print 'Downloading file:',filePath
            #sftp.put(os.path.join(local_dir,f),os.path.join(remote_dir,f))#上传
            print 'Download file success %s ' % datetime.datetime.now()
            print ''
            print '##########################################'
        except Exception as e:
               print "connect error!", e
        finally:
            t.close()

    def upload_file(self, filePath):
        try:
            t=paramiko.Transport((self.host,self.port))
            t.connect(username=self.user, password=self.password)
            sftp=paramiko.SFTPClient.from_transport(t)
            tmpFile = os.path.basename(filePath)
            print ''
            print '#########################################'
            print 'Beginning to upLoad file  from localhost to %s %s %s ' % (self.host, filePath, datetime.datetime.now())
            print 'Uploading file:',filePath
            sftp.put(filePath, tmpFile)#上传
            print 'Download file success %s ' % datetime.datetime.now()
            print ''
            print '##########################################'
        except Exception as e:
               print "connect error!", e
        finally:
            t.close()


# if __name__ == '__main__':
#
#     parser = argparse.ArgumentParser(description='msyql Data transfer')
#     parser.add_argument('--h', help='the host of sourceServer', default='192.168.1.102')
#     parser.add_argument('--P', help='the port of ssh', default=22, type=int)
#     parser.add_argument('--u', help='the user of the source server', default='zy')
#     parser.add_argument('--p', help='the password of source server', default='abcd1234')
#     parser.add_argument('--database', help='the database to be transfer', default='dumptest')
#     parser.add_argument('--sh', help='the host of sourceServer', default='192.168.1.102')
#     parser.add_argument('--sP', help='the port of ssh', default=22, type=int)
#     parser.add_argument('--su', help='the user of the source server', default='zy')
#     parser.add_argument('--sp', help='the password of source server', default='abcd1234')
#
#     args = parser.parse_args()
#
#     # 执行远程命令
#     shell = RemoteShell(args.h, args.P, args.u, args.p)
#
#     shell.executeCMD(DUMP_COMMAND)
#     # 下载文件
#     shell.downLoadFile('/home/zy/dumptest.sql')
#
#     # 上传文件
#     destShell = RemoteShell(args.sh, args.sP, args.su, args.sp)
#     destShell.upLoadFile('C:/Users/Administrator/dumptest.sql')
#     destShell.executeCMD(IMPORT_COMMAND)