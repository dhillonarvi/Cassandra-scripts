#!/usr/bin/python

import glob
import os
import socket
import subprocess
import time
import logging
import logging.handlers

###################### Step 0: Establish parameters ########################

snapshot_tag = time.strftime("backup%Y%m%dT%H%M%SZ", time.gmtime())
data_directory = '/var/lib/cassandra/data'
pickup_directory = '/var/lib/cassandra/backup'
backup_basename = '{}-{}'.format(socket.gethostname().split('.')[0], snapshot_tag)
tarball_name = '{}.tar.gz'.format(backup_basename)
schema_file = 'schema-{}.cql'.format(snapshot_tag)
log_file = '/var/log/cassandra/backuplogs.log'
host_name = os.uname()[1]
email_addr = 'arvinder.singh@gmail.com'
dev_null = open('/dev/null', 'w')

#rotate log files:
should_roll_over = os.path.isfile(log_file)
handler = logging.handlers.RotatingFileHandler(log_file, mode='a', backupCount=8)
if should_roll_over:
    handler.doRollover()
logging.basicConfig(filename=log_file,level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s",mode='w')

if not os.path.exists(pickup_directory):
    os.mkdir(pickup_directory)
os.chdir(data_directory)

###################### Step 1: Take snapshot & schema dump ##################

try:
    logging.info("\n\nStarting snapshot and schema backup with tag %s", snapshot_tag)
    IGNORED_KEYSPACES = set(['system', 'system_auth', 'system_distributed', 'system_traces'])
    keyspaces = [x for x in os.listdir('.') if x not in IGNORED_KEYSPACES]
    logging.info("starting snapshot for keyspace: %s",x)
    with open(log_file,'a') as f1:
        subprocess.check_call(['/opt/cassandra/bin/nodetool', '-u', 'root', '-pwf', 'jmxremote.password.file',  'snapshot', '-t', snapshot_tag, '--'] + keyspaces,stdout=dev_null, stderr=f1)
    logging.info("Starting schema backups...")
    with open(schema_file, 'wb') as f:
        subprocess.check_call(['/opt/cassandra/bin/cqlsh', '-e', 'DESCRIBE SCHEMA'], stdout=f)
    logging.info("snapshot and schema backup completed.")

###################### Step 2: Tarball the snapshot #########################

    logging.info("Starting tarball the snapshot...")
    each_cf_dir = glob.glob('*/*/snapshots/' + snapshot_tag)
    subprocess.check_call(['tar', '--warning=none', '-zcf', '/var/tmp/' + tarball_name, '--xform=s!/snapshots/[^/]*!!;s!^!{}/!'.format(backup_basename), '--', schema_file] + each_cf_dir)
    logging.info("tarballed the snapshot.")

###################### Step 3: Clear snapshot & schema dump #################

    logging.info("Deleting old snapshos...")
    subprocess.check_call(['/opt/cassandra/bin/nodetool', '-u', 'root', '-pwf', 'jmxremote.password.file', 'clearsnapshot', '-t', snapshot_tag], stdout=dev_null)
    logging.info("Old Snapshots deleted.")
    os.unlink(schema_file)
    logging.info("Deleted schema dump from data directory.")

###################### Step 4: Expose snapshot for pickup ###################

    logging.info("Moving backup to bkp directory...")
    os.rename('/var/tmp/' + tarball_name, pickup_directory + '/' + tarball_name)
    logging.info("backup is ready for pickup by opsarchiver.")
except Exception as e:
    logging.info("Backup failed with error: %s",e)
    log_in = open(log_file,'r')
    subprocess.check_call(['mail','-r', 'noreply', '-s', 'cassandra backup failed on '+ host_name, email_addr],stdin=log_in)
