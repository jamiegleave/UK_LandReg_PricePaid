import os
from glob import glob
import requests
import subprocess
from tqdm import tqdm
from datetime import datetime as dt
import mysql.connector

def define_prefix():
    return str(dt.today().day)+'_'+str(dt.today().month)+'_'+str(dt.today().year)


def fetch_csv(url, dest):
    file_prefix = define_prefix()

    response = requests.get(url,stream=True)

    total_size = int(response.headers.get('content-length',0))
    block_size = 1024

    with tqdm(total=total_size,unit='B',unit_scale=True) as progress_bar:
        with open(dest+file_prefix+'_pricepaid_complete.csv','wb') as file:
            for chunk in response.iter_content(block_size):
                progress_bar.update(len(chunk))
                file.write(chunk)
            file.flush()


def check_vers(url, dest):
    response = requests.get(url,stream=True)
    server_sz = int(response.headers.get('content-length',0))
    
    print('Server file is'+' '+str(round(server_sz/1000000000,5))+' '+'GB')

    try:
        local_sz = os.path.getsize(max(glob(dest+'*'),key=os.path.getctime))
        print('Local file is'+' '+str(round(local_sz/1000000000,5))+' '+'GB')
        if local_sz == server_sz:
            print('Local file is up-to-date')
        else:
            print('Updated version available on server - initiating download')
            fetch_csv(url,dest)
            
    except:
        print('No file exists - initiating download')
        fetch_csv(url,dest)


def copy_file_to_docker_volume(file_name, container_name, local_path, mount_path):

    destination_path = f"{container_name}:{mount_path}"
    subprocess.run(["docker", "cp", './'+file_name, destination_path], check=True,cwd=local_path)

    print(f"Successfully copied {local_path} to path {mount_path} (container {container_name})")


def sql_commitchanges(query):
    cnx = mysql.connector.connect(user='root',password='password',
                              host = 'localhost',
                              port = 3306,
                              database='CSV_DB 7')
    
    cursor = cnx.cursor(buffered=True)
    cursor.execute(query)
    cnx.commit()
    cnx.close()