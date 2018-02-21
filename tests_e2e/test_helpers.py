import os
import shutil
from subprocess import call
import psycopg2


def run_pipe():
    call(['./remove_done_files.sh'])
    call(['./e2e_transmart_only.sh'])


def clean_database(config):
    # set env vars
    os.environ['PGPORT'] = config['GlobalConfig']['PGPORT']
    os.environ['PGDATABASE'] = config['GlobalConfig']['PGDATABASE']
    os.environ['PGUSER'] = config['E2eTest']['PGADMINUSER']
    os.environ['PGPASSWORD'] = config['E2eTest']['PGADMINPASSWORD']
    os.environ['TABLESPACES'] = os.environ['HOME'] + '/pg/tablespaces/'

    # drop and build transmart database
    call(['make', 'postgres_drop', '-C', config['E2eTest']['transmart_data']])
    call(['make', 'postgres', '-C', config['E2eTest']['transmart_data']])


def clean_dropzone(config):
    if os.path.exists(config['GlobalConfig']['drop_dir']):
        shutil.rmtree(config['GlobalConfig']['drop_dir'])
    if not os.path.exists(config['GlobalConfig']['drop_dir']):
        shutil.copytree(config['E2eTest']['parent_drop_dir'], config['GlobalConfig']['drop_dir'])


def checkDB(config):
    cs = "dbname=%s user=%s password=%s port=%s" % \
         (config['GlobalConfig']['PGDATABASE'], config['GlobalConfig']['PGUSER'],
          config['GlobalConfig']['PGPASSWORD'], config['GlobalConfig']['PGPORT'])
    conn = psycopg2.connect(cs)
    cur = conn.cursor()
    cur.execute('SELECT * FROM i2b2demodata.study;')
    first_row = cur.fetchone()
    cur.close()
    conn.close()
    return first_row