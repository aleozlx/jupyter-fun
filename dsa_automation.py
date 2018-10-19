import os, sys, subprocess, uuid
import datetime
import sqlite3
import requests
from ipywidgets import Button, HBox, IntProgress
import IPython.display
from IPython.display import Javascript, HTML, display, clear_output

scary_template = 'IPython.notebook.kernel.execute("{}");'
localdb = sqlite3.connect('local.db')

def scary(code):
    return Javascript(scary_template.format(code))

def scarier(code):
    return Javascript(scary_template.replace('"', "`").format(code))

display(scarier("__notebook_path='${IPython.notebook.notebook_path}'; dsa_automation.notebook_path=__notebook_path"))

def inside_docker():
    docker_flag = os.system("grep -q docker /proc/1/cgroup")==0
    return docker_flag

def nbid():
    """ This function must be called from a Jupyter kernel.
    It will securely generate an identifier of the caller to the current working directory.
    Many ways of illegal access will be detected by the subprocess, that will halt unobtrusively for self-protection.
    """
    subprocess.Popen(
        ['/dsa/data/scripts/nbid', notebook_path],
        stdout=subprocess.PIPE
    ).stdout.read().decode('ascii')

def aml_list(path, excludes = ['.ipynb_checkpoints', '__pycache__', 'local.db'], exclude_exts = ['.ipynb', '.tgz']):
    return [fname for fname in set(os.listdir(path)) - set(excludes) if os.path.splitext(fname)[1] not in exclude_exts]

def aml_archive(fnames):
    track_id = uuid.uuid1()
    os.system('tar cvzf {}.tgz {}'.format(track_id, '\x20'.join(fnames)))
    return track_id

def aml_submit():
    nbid()
    files = aml_list('.')
    track_id = aml_archive(files)
    fname_sub = '{}.tgz'.format(track_id)
    res = requests.post('http://128.206.117.147:5000/sub/{}'.format(track_id),
        files= {'file': (fname_sub, open(fname_sub, 'rb'))}, timeout=5).json()
    return track_id, files, res

def aml_onsubmit(btn=None):
    track_id, files, res = aml_submit()
    # print(res)
    localdb.execute('INSERT INTO my_submissions VALUES (?, ?, "unknown", ?);',
        (str(track_id), str(files), datetime.datetime.now()))
    localdb.commit()
    aml_onrefresh()

def aml_onrefresh(btn=None):
    ret = localdb.execute("SELECT track_id FROM my_submissions WHERE state='unknown';").fetchone()
    if ret:
        (track_id, ) = ret
    else: return
    res = requests.get('http://128.206.117.147:5000/r/{}'.format(track_id), timeout=5).json()
    if res['status'] in {'ok', 'err'}:
        localdb.execute("UPDATE my_submissions SET state=? WHERE track_id=?;", (res['status'], track_id))
        localdb.commit()
        clear_output()
        ui_amljob(False)
        if res['status']=='ok':
            os.system('wget http://128.206.117.147:5000/f/{} -O {}'.format(track_id, res['data']))

def ui_amljob(init=True):
    if init:
        btnSubmit = Button(description='New submission', button_style='primary')
        btnSubmit.on_click(aml_onsubmit)
        btnRefresh = Button(description='Refresh', button_style='primary')
        btnRefresh.on_click(aml_onrefresh)
        localdb.execute("""CREATE TABLE IF NOT EXISTS my_submissions (
            track_id text,
            files text,
            state text,
            ts timestamp
        );""")
        display(HBox([btnSubmit, btnRefresh]))
    import pandas as pd
    submissions = pd.DataFrame(localdb.execute("SELECT track_id, ts, state FROM my_submissions;").fetchall(),
        columns=['id', 'time', 'state'])
    display(HTML(submissions.to_html()))

# EMR things stole from @blackwoodm and sugar-coated

def emr_config(fname):
    import yaml
    with open(fname, 'r') as f:
        return yaml.load(f)

def emr_newcluster(btn):
    import getpass, os, json, time, datetime
    ctx = dict() # context variables that will be persisted in a local database
    ctx.update(config)
    ctx['system_user_name'] = getpass.getuser()
    ctx['wk_dir'] = os.getcwd()

    # Create SSH Keypair
    ctx['emr_pem_file'] = time.strftime('EMR-%d%m%Y%H%M%S-{system_user_name}'.format(**ctx))
    global emr_key
    emr_key = ec2.create_key_pair(KeyName=ctx['emr_pem_file'])
    ctx['emr_key']=json.dumps(emr_key)
    ctx['KeyMaterial'] = emr_key['KeyMaterial']
    os.system('echo "{KeyMaterial}" > {emr_pem_file}.pem'.format(**ctx))
    del ctx['KeyMaterial']
    os.chmod('{wk_dir}/{emr_pem_file}.pem'.format(**ctx), 0o400)

    # Launch EMR Cluster
    response = emr.run_job_flow(
        Name='EMR Jupyter NB-{system_user_name}'.format(**ctx),
        LogUri='s3n://logs-{system_user_name}/elasticmapreduce/'.format(**ctx),
        ReleaseLabel='emr-5.17.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name':'Master - 1',
                    'InstanceRole':'MASTER',
                    'InstanceType':config['instance_size'],
                    'InstanceCount':config['master_instances'],
                },
                {
                    'Name':'Core - 2',
                    'InstanceRole':'CORE',
                    'InstanceType':config['instance_size'],
                    'InstanceCount':config['master_instances'],
                },
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName': ctx['emr_pem_file'],
            'Placement': {
                'AvailabilityZone': 'us-west-2c'
            }
        },

        AutoScalingRole="EMR_AutoScaling_DefaultRole",
        Applications=[
            { 'Name': 'Hadoop' },
            { 'Name': 'Hive' },
            { 'Name': 'Spark' },
            { 'Name': 'Pig' },
            { 'Name': 'JupyterHub' }
        ],
        Configurations=[
            {
                'Classification': 'spark',
                'Configurations': [],
                'Properties': {
                    'maximizeResourceAllocation':'true'
                }
            },
        ],
        VisibleToAllUsers=False,
        EbsRootVolumeSize=10,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
    ) #End of Cluster Launch Command

    cluster_id = response['JobFlowId']
    localdb.execute('INSERT INTO my_clusters VALUES (?, "unknown", ?);',
        (cluster_id, datetime.datetime.now()))
    localdb.commit()

    response = emr.describe_cluster(
        ClusterId=cluster_id
    )
    while True:
        response = emr.describe_cluster(ClusterId=cluster_id)
        try:
            response['Cluster']['MasterPublicDnsName'].find("ec2")
            print('...Cluster DNS Active',end="")
            break
        except:
            time.sleep(5)
            print(".", end="")
            pass
    print("\n\nProceeding with Firewall Rules...")
    #Get Cluster Security Group Info
    master_security_group = response['Cluster']['Ec2InstanceAttributes']['EmrManagedMasterSecurityGroup']
    slave_security_group = response['Cluster']['Ec2InstanceAttributes']['EmrManagedSlaveSecurityGroup']

    ctx['master_name'] = response['Cluster']['MasterPublicDnsName']
    for k, v in ctx.items():
        localdb.execute('INSERT INTO my_clusters_facts VALUES (?, ?, ?);', (cluster_id, k, v))
    localdb.commit()

    def add_security_group(group_id, name, port):
        try:
            data = ec2.authorize_security_group_ingress(
                GroupId=group_id,
                IpPermissions=[
                    {'IpProtocol': 'tcp',
                    'FromPort': port,
                    'ToPort': port,
                    'IpRanges': [{'CidrIp': '128.206.0.0/16'}]},
                ])
            print("Ingress {} added".format(name))
        except:
            print("Ingress {} already added".format(name))

    firewall_allow_list = [
        (master_security_group, 'SSH', 22),
        (master_security_group, 'YARN', 8088),
        (master_security_group, 'HDFS NameNode', 50070),
        (master_security_group, 'Spark History Server', 18080),
        (master_security_group, 'Hue', 8888),
        (master_security_group, 'HBase', 16010),
        (master_security_group, 'Jupyter Notebook', 9090),
        (master_security_group, 'Hue', 8888),
        (slave_security_group, 'Slave SSH', 22),
        (slave_security_group, 'Slave YARN NodeManager', 8042),
        (slave_security_group, 'Slave HDFS DataNode', 50075),
        
    ]

    print ("\n\nFinishing Startup.\nThis will take a few minutes...\n\n***Please Wait***\n\nStarting.",end="")

    while str(response['Cluster']['Status']['State']) == 'STARTING':
            time.sleep(5)
            print(".", end="")
            response = emr.describe_cluster(
                ClusterId=cluster_id
            )
    print('...Done',end="")

    print ("\n\nRunning Bootstrap Actions.\nThis will take a few minutes...\n\n***Please Wait***\n\nBootstrapping.",end="")

    while str(response['Cluster']['Status']['State']) == 'BOOTSTRAPPING':
            time.sleep(5)
            print(".", end="")
            response = emr.describe_cluster(
                ClusterId=cluster_id
            )
    print('...Done',end="")
    # print('\n\nCluster Status: '+response['Cluster']['Status']['State'])

    #Refresh Cluster Description
    response = emr.describe_cluster(
        ClusterId=cluster_id
    )

    #Bootstrap Cluster with Fabric
    from fabric import tasks
    from fabric.api import run
    from fabric.api import env
    from fabric.api import hide
    from fabric.network import disconnect_all

    env.host_string = ctx['master_name']
    env.user = 'hadoop'
    env.key_filename = '{wk_dir}/{emr_pem_file}.pem'.format(**ctx)
    env.warn_only
    os.system('StrictHostKeyChecking=no -r -i {wk_dir}/{emr_pem_file}.pem {wk_dir}/{load_notebook_location} hadoop@{master_name}:/var/lib/jupyter/home/jovyan')
    print('Everything is ready!')

def emr_onrefresh(btn):
    while 1:
        ret = localdb.execute("SELECT cluster_id FROM my_clusters WHERE state='unknown' or state='ready';").fetchone()
        if ret:
            (cluster_id, ) = ret
        else: break
        res = emr.describe_cluster(ClusterId=cluster_id)
        if res['Cluster']['Status']['State'] == 'WAITING':
            localdb.execute("UPDATE my_clusters SET state=? WHERE cluster_id=?;", ('ready', cluster_id))
        elif res['Cluster']['Status']['State'] == 'TERMINATED':
            localdb.execute("UPDATE my_clusters SET state=? WHERE cluster_id=?;", ('terminated', cluster_id))
    localdb.commit()
    clear_output()
    ui_emr(False)

def emr_onterminate(btn):
    ret = localdb.execute("SELECT cluster_id FROM my_clusters WHERE state='unknown' or state='ready';").fetchone()
    if ret:
        (cluster_id, ) = ret
    else: return
    emr.set_termination_protection(
        JobFlowIds=[ cluster_id ],
        TerminationProtected=False
    )
    res = emr.terminate_job_flows(
        JobFlowIds=[ cluster_id ]
    )
    ret = localdb.execute("SELECT v FROM my_clusters_facts WHERE cluster_id=? AND k=?;", (cluster_id, 'emr_pem_file')).fetchone()
    if ret:
        (emr_pem_file, ) = ret
    else: return
    res = ec2.delete_key_pair(KeyName=emr_pem_file)

def ui_emr(init=True):
    if init:
        btnNew = Button(description='New EMR Cluster', button_style='primary')
        btnNew.on_click(emr_newcluster)
        btnRefresh = Button(description='Refresh', button_style='primary')
        btnRefresh.on_click(emr_onrefresh)
        btnTerminate = Button(description='Terminate', button_style='danger')
        btnTerminate.on_click(emr_onterminate)
        localdb.execute("""CREATE TABLE IF NOT EXISTS my_clusters (
            cluster_id text,
            state text,
            ts timestamp
        );""")
        localdb.execute("""CREATE TABLE IF NOT EXISTS my_clusters_facts (
            cluster_id test,
            k text,
            v text
        );""")
        display(HBox([btnNew, btnRefresh, btnTerminate]))

        # this does not support multi-cluster due to singleton pattern
        import boto3
        global emr, ec2
        config = emr_config('aws-emr-config.yml')
        secrets = emr_config('aws-emr-secrets.yml') # git-ignored
        emr = boto3.client('emr',
            region_name=config['region'],
            aws_access_key_id=secrets['access_id'],
            aws_secret_access_key=secrets['access_key']
        )
        ec2 = boto3.client('ec2',
            region_name=config['region'],
            aws_access_key_id=secrets['access_id'],
            aws_secret_access_key=secrets['access_key']
        )
    import pandas as pd
    clusters = pd.DataFrame(localdb.execute("SELECT cluster_id, ts, state FROM my_clusters;").fetchall(),
        columns=['cluster_id', 'time', 'state'])
    display(HTML(clusters.to_html()))


