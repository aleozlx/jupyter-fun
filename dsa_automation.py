import os, sys, subprocess, uuid, time
import datetime
import sqlite3
import requests
import threading
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

def aml_list(path, excludes = ['.ipynb_checkpoints', '__pycache__', 'local.db'], exclude_exts = ['.tgz', '.h5']):
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
    ret = localdb.execute('SELECT COUNT(*) AS ct_unfinished FROM my_submissions WHERE state!="ok" and state!="err";').fetchone()
    if ret:
        (ct_unfinished, ) = ret
        if ct_unfinished > 0: # deny concurrent submissions
            return
    track_id, files, res = aml_submit()
    # print(res)
    localdb.execute('INSERT INTO my_submissions VALUES (?, ?, "unknown", ?);',
        (str(track_id), str(files), datetime.datetime.now()))
    try_commitdb()
    aml_onrefresh()

def aml_onrefresh(btn=None):
    ret = localdb.execute("SELECT track_id FROM my_submissions WHERE state!='ok' and state!='err';").fetchone()
    if ret:
        (track_id, ) = ret
    else: return
    res = requests.get('http://128.206.117.147:5000/r/{}'.format(track_id), timeout=5).json()
    if res['status'] in {'ok', 'err'}:
        localdb.execute("UPDATE my_submissions SET state=? WHERE track_id=?;", (res['status'], track_id))
        try_commitdb()
        clear_output()
        ui_amljob(False)
        if res['status']=='ok':
            os.system('wget http://128.206.117.147:5000/f/{} -O {}'.format(track_id, res['data']))
    elif res['status'] == 'pending':
        localdb.execute("UPDATE my_submissions SET state=? WHERE track_id=?;", (res['msg'], track_id))
        try_commitdb()
        clear_output()
        ui_amljob(False)

def aml_onrevoke(btn=None):
    ret = localdb.execute("SELECT track_id FROM my_submissions WHERE state!='ok' and state!='err';").fetchone()
    if ret:
        (track_id, ) = ret
    else: return
    print('terminating task {}'.format(track_id))
    res = requests.post('http://128.206.117.147:5000/del/{}'.format(track_id), timeout=5).json()

def ui_amljob(init=True):
    if init:
        btnSubmit = Button(description='New submission', button_style='primary')
        btnSubmit.on_click(aml_onsubmit)
        btnRefresh = Button(description='Refresh', button_style='primary')
        btnRefresh.on_click(aml_onrefresh)
        btnRevoke = Button(description='Revoke', button_style='danger')
        btnRevoke.on_click(aml_onrevoke)
        localdb.execute("""CREATE TABLE IF NOT EXISTS my_submissions (
            track_id text,
            files text,
            state text,
            ts timestamp
        );""")
        display(HBox([btnSubmit, btnRefresh, btnRevoke]))
    import pandas as pd
    submissions = pd.DataFrame(localdb.execute("SELECT track_id, ts, state FROM my_submissions;").fetchall(),
        columns=['id', 'time', 'state'])
    display(HTML(submissions.to_html()))

def tf_limit(tf, n, p=None):
    if p:
        gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=p)
    else:
        gpu_options = tf.GPUOptions()

    tfconfig = tf.ConfigProto(
        # set both to be equal to PHYSICAL cores for optimal performance
        intra_op_parallelism_threads=n,
        inter_op_parallelism_threads=n,
        allow_soft_placement=True,
        gpu_options = gpu_options
    )
    return tfconfig

# EMR things stole from @blackwoodm and sugar-coated

def get_emr_config(fname):
    import yaml
    with open(fname, 'r') as f:
        return yaml.load(f)

def try_commitdb(retries = 5):
    for i in range(retries):
        try: # Sqlite vs NFS
            localdb.commit()
            break
        except: # OperationalError disk I/O error
            time.sleep(1)

def emr_newcluster(btn):
    import getpass, os, json, time, datetime
    ctx = dict() # context variables that will be persisted in a local database
    # ctx.update(emr_config)
    ctx['system_user_name'] = getpass.getuser()
    ctx['wk_dir'] = os.getcwd()
    progress = IntProgress(description='New EMR cluster', min=0, max=100)
    display(progress)
    progress.value = 5

    # Create SSH Keypair
    ctx['emr_pem_file'] = time.strftime('EMR-%d%m%Y%H%M%S-{system_user_name}'.format(**ctx))
    emr_key = ec2.create_key_pair(KeyName=ctx['emr_pem_file'])
    ctx['emr_key']=json.dumps(emr_key)
    os.system('echo "{KeyMaterial}" > {emr_pem_file}.pem'.format(KeyMaterial=emr_key['KeyMaterial'], **ctx))
    os.chmod('{wk_dir}/{emr_pem_file}.pem'.format(**ctx), 0o400)
    progress.value = 7

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
                    'InstanceType':emr_config['instance_size'],
                    'InstanceCount':emr_config['master_instances'],
                },
                {
                    'Name':'Core - 2',
                    'InstanceRole':'CORE',
                    'InstanceType':emr_config['instance_size'],
                    'InstanceCount':emr_config['slave_instances'],
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
            # { 'Name': 'Pig' },
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
    progress.value = 10

    cluster_id = response['JobFlowId']
    localdb.execute('INSERT INTO my_clusters VALUES (?, "unknown", ?);',
        (cluster_id, datetime.datetime.now()))
    for k, v in ctx.items():
        localdb.execute('INSERT INTO my_clusters_facts VALUES (?, ?, ?);', (cluster_id, k, v))
    try_commitdb()

    print("Provisioning a new EMR cluster from AWS. Please do not refresh until it is done.")
    while 1:
        response = emr.describe_cluster(ClusterId=cluster_id)
        try:
            response['Cluster']['MasterPublicDnsName'].find("ec2")
            print('...Cluster DNS Active',end="")
            break
        except:
            time.sleep(10)
            print(".", end="")
            pass
    progress.value = 15
    
    ctx['master_name'] = response['Cluster']['MasterPublicDnsName']
    localdb.execute('INSERT INTO my_clusters_facts VALUES (?, "master_name", ?);', (cluster_id, ctx['master_name']))
    try_commitdb()

    def background_emr_provision():
        print("\n\nProceeding with Firewall Rules...")
        progress.description = 'Tweaking firewall...'
        #Get Cluster Security Group Info
        response = emr.describe_cluster(ClusterId=cluster_id) # for the sake of var scope
        master_security_group = response['Cluster']['Ec2InstanceAttributes']['EmrManagedMasterSecurityGroup']
        slave_security_group = response['Cluster']['Ec2InstanceAttributes']['EmrManagedSlaveSecurityGroup']

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
            progress.value += 2

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
        for rule in firewall_allow_list:
            add_security_group(*rule)
        progress.value = 50
        progress.description = 'Starting instances...'

        print ("\n\nFinishing Startup.\nThis will take a few minutes...\n\n***Please Wait***\n\nStarting.",end="")

        while str(response['Cluster']['Status']['State']) == 'STARTING':
                time.sleep(10)
                print(".", end="")
                response = emr.describe_cluster(
                    ClusterId=cluster_id
                )
        print('...Done',end="")
        progress.value = 80
        progress.description = 'Bootstrapping...'

        print ("\n\nRunning Bootstrap Actions.\nThis will take a few minutes...\n\n***Please Wait***\n\nBootstrapping.",end="")

        while str(response['Cluster']['Status']['State']) == 'BOOTSTRAPPING':
                time.sleep(5)
                print(".", end="")
                response = emr.describe_cluster(
                    ClusterId=cluster_id
                )
        print('...Done',end="")
        progress.value = 90
        # print('\n\nCluster Status: '+response['Cluster']['Status']['State'])      

        #Refresh Cluster Description
        response = emr.describe_cluster(
            ClusterId=cluster_id
        )

        # volume metadata: ec2.Volume(id)
        progress.description = 'Mounting EBS...'
        master_instance = [i for i in emr.list_instances(ClusterId=cluster_id)['Instances'] if i['PublicDnsName']==ctx['master_name']][0]
        # home_volume = ec2.create_volume(AvailabilityZone=response['Cluster']['Ec2InstanceAttributes']['Ec2AvailabilityZone'], Size=8)
        ec2.attach_volume(InstanceId=master_instance['Ec2InstanceId'], VolumeId=emr_map_ebs(ctx['system_user_name']), Device='/dev/xvdz')
        
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
        # os.system('StrictHostKeyChecking=no -r -i {wk_dir}/{emr_pem_file}.pem {wk_dir}/{load_notebook_location} hadoop@{master_name}:/var/lib/jupyter/home/jovyan'.format(
        #     load_notebook_location=emr_config['load_notebook_location'], **ctx))
        # just in case https://jpetazzo.github.io/2015/01/13/docker-mount-dynamic-volumes/
        with hide('output'):
            run('sudo docker restart jupyterhub')
            run('sudo file -s /dev/xvdz | grep -q ext4 || sudo mkfs.ext4 /dev/xvdz')
            run('sudo docker exec jupyterhub mkdir /home/jovyan/workspace')
            run('sudo docker exec jupyterhub touch /home/jovyan/workspace/\'danger!!\'')
            run('sudo docker exec jupyterhub mount /dev/xvdz /home/jovyan/workspace')
            run('sudo docker exec jupyterhub chown jovyan:users /home/jovyan/workspace')
        
        progress.value = 100
        progress.description = 'Done.'
        print('Everything is ready!')
        
    t_background_emr_provision = threading.Thread(target=background_emr_provision)
    t_background_emr_provision.start()

def emr_map_ebs(sso=None):
    if sso is None:
        import getpass
        sso = getpass.getuser()
    ebs_mapping = {'zy5f9': 'vol-08d67fa11f94f963b'}
    return ebs_mapping[sso]

def emr_onrefresh(btn):
    ret = localdb.execute("SELECT cluster_id FROM my_clusters WHERE state!='terminated';").fetchone()
    if ret:
        (cluster_id, ) = ret
    else: return
    res = emr.describe_cluster(ClusterId=cluster_id)
    if res['Cluster']['Status']['State'] == 'WAITING':
        localdb.execute("UPDATE my_clusters SET state=? WHERE cluster_id=?;", ('ready', cluster_id))
    else:
        localdb.execute("UPDATE my_clusters SET state=? WHERE cluster_id=?;", (res['Cluster']['Status']['State'].lower(), cluster_id))
    try_commitdb()
    clear_output()
    ui_emr(False)

def emr_onterminate(btn):
    ret = localdb.execute("SELECT cluster_id FROM my_clusters WHERE state!='terminated' and state!='terminating';").fetchone()
    if ret:
        (cluster_id, ) = ret
    else: return
    print('Terminating the cluster.')
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
    os.remove('{}.pem'.format(emr_pem_file))

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

        import boto3
        global emr, ec2, emr_config # this does not support multi-cluster due to the singleton pattern here
        emr_config = get_emr_config('aws-emr-config.yml')
        emr_config.update(get_emr_config('aws-emr-secrets.yml')) # git-ignored sensitive info
        emr = boto3.client('emr',
            region_name=emr_config['region'],
            aws_access_key_id=emr_config['access_id'],
            aws_secret_access_key=emr_config['access_key']
        )
        ec2 = boto3.client('ec2',
            region_name=emr_config['region'],
            aws_access_key_id=emr_config['access_id'],
            aws_secret_access_key=emr_config['access_key']
        )
    import pandas as pd
    clusters = pd.DataFrame(localdb.execute("SELECT cluster_id, ts, state FROM my_clusters;").fetchall(),
        columns=['cluster_id', 'time', 'state'])
    display(HTML(clusters.to_html()))

def ui_emr_services(cluster_id=None):
    if cluster_id is None:
        ret = localdb.execute("SELECT cluster_id FROM my_clusters WHERE state!='terminated' and state!='terminating';").fetchone()
        if ret:
            (cluster_id, ) = ret
        else:
            return HTML('No running cluster.')
    ret = localdb.execute("SELECT k,v FROM my_clusters_facts WHERE cluster_id=?;", (cluster_id, )).fetchall()
    if ret:
        ctx = {k:v for k,v in ret}
        return HTML("""
        <p><strong>Log into your dedicated Jupyter from AWS EMR Cluster</strong></p>
        <a class="jupyter-widgets jupyter-button widget-button mod-primary" href="https://{master_name}:9443/" target="_blank">Jupyter Notebook</a>
        <blockquote><strong>Username: </strong>jovyan <strong>Password: </strong>jupyter
        <p>
          Chrome will suggest that the connection is not private, but it is. This appears this way because we are using self-signed cert that is not recognized by Cert Authorities.
          It is safe to proceed by clicking the "ADVANCED" link at the left bottom of the page and then the "Proceed to ...compute.amazonaws.com (unsafe)" link.
        </p>
        </blockquote>
        <hr/>
        <p><strong>Or check out the Hadoop ecosystem provided by AWS EMR</strong></p>
        <a class="jupyter-widgets jupyter-button widget-button" href="http://{master_name}:8088/" target="_blank">YARN</a>
        <a class="jupyter-widgets jupyter-button widget-button" href="http://{master_name}:50070/" target="_blank">HDFS</a>
        <a class="jupyter-widgets jupyter-button widget-button" href="http://{master_name}:18080/" target="_blank">Spark History</a>
        <!--
        <a class="jupyter-widgets jupyter-button widget-button" href="https://{master_name}:8888/" target="_blank">Hue</a>
        <a class="jupyter-widgets jupyter-button widget-button" href="https://{master_name}:16010/" target="_blank">HBase</a>
        -->
        <hr/>
        <p><strong>Or SSH access to the master node from a
            <a href="https://jupyterhub.dsa.missouri.edu/user/{system_user_name}/terminals/1" target="_blank">terminal</a></strong></p>
        <pre>ssh -i {wk_dir}/{emr_pem_file}.pem hadoop@{master_name}</pre>
        """.format(**ctx))
    else:
        return HTML('Cluster not found.')
    
