import pandas as pd
import requests
import subprocess
from tqdm import tqdm
from datetime import datetime as dt
import psycopg2


def get_req(url):
    
    response = requests.get(url,stream=True)
    last_mod = dt.strptime(response.headers.get('Last-Modified'),'%a, %d %b %Y %H:%M:%S %Z')
    return {
        'response': response,
        'last_mod': last_mod,
        'file_name': last_mod.strftime('%b')+str(last_mod.strftime('%y'))+'pricepaidcomplete',
        'file_sz':int(response.headers.get('content-length',0))
    }


def fetch_csv(url, dest):
    
    block_size = 1024
    resp_dict = get_req(url)
    
    with tqdm(total=resp_dict['file_sz'],unit='B',unit_scale=True) as progress_bar:
        with open(dest+resp_dict['file_name']+'.csv','wb') as file:
            for chunk in resp_dict['response'].iter_content(block_size):
                progress_bar.update(len(chunk))
                file.write(chunk)
            file.flush()

    return resp_dict['file_name']


def copy_file_to_docker_volume(file_name, container_name, local_path, mount_path):

    destination_path = f"{container_name}:{mount_path}"
    subprocess.run(["docker", "cp", './'+file_name, destination_path], check=True,cwd=local_path)

    print(f"Successfully copied {local_path} to path {mount_path} (container {container_name})")


def postgresql_commitchanges(query,host,database,user,password):
    
    cnx = psycopg2.connect(host=host,database=database,user=user,password=password)
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    cnx.close()


def postgresql_extractdata(query,host,database,user,password):
    
    cnx = psycopg2.connect(host=host,database=database,user=user,password=password)
    cursor = cnx.cursor()
    cursor.execute(query)
    return cursor.fetchall()
