import subprocess
import sys
import os
import configparser
import math
import shutil

# Usage: python main.py <TorrentID> <Content folder name>

def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

QUEUE_SOCKS = {"disk": "/tmp/tsp-disk.sock", "network": "/tmp/tsp-network.sock"}

def execute(command, queue=None):
    env = os.environ.copy()
    if queue is not None:
        if queue in QUEUE_SOCKS:
            env["TS_SOCKET"] = QUEUE_SOCKS[queue]
            command = [config['toolchain']['tsp'], "-n", "-f"] + command
        else:
            print("Warning: unknown queue option '%s' for execute(), run the task immediately." % queue)
    print("Executing:", command)
    process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    
    return process.returncode

def escape_name(orig_name):
    #for OneDrive filename rules
    res = orig_name.replace('~', "[tilde]")
    res = res.replace('"', "[quote]")
    res = res.replace('#', "[sharp]")
    res = res.replace('%', "[pct]")
    res = res.replace('&', "[and]")
    res = res.replace('*', "[star]")
    res = res.replace(':', "[colon]")
    res = res.replace('<', "[langle]")
    res = res.replace('>', "[rangle]")
    res = res.replace('?', "[qmark]")
    res = res.replace('/', "[slash]")
    res = res.replace('\\', "[rslash]")
    res = res.replace('{', "[lcurly]")
    res = res.replace('}', "[rcurly]")
    res = res.replace('|', "[vbar]")
    return res

def main():
    if not os.path.exists('./config.ini'):
        print("Please run bootstrap.py first")
        raise FileNotFoundError

    global config
    config = configparser.ConfigParser()
    config.read('./config.ini')

    category_folder = sys.argv[1]
    full_content_path = sys.argv[2]
    folder_name = os.path.basename(full_content_path)
    if folder_name == "":
        folder_name = os.path.basename(os.path.dirname(full_content_path))
    orig_folder_name = folder_name
    folder_name = escape_name(folder_name)
    backup_path = os.path.join(config['misc']['prefix'], folder_name)

    full_path_size = get_size(full_content_path) / 1024.0 / 1024 / 1024
    max_size = full_path_size

    if config['rclone']['compress_account'] != "":

        # Compress folder
        os.makedirs(backup_path, exist_ok=True)
        rar_cmd = [config['toolchain']['rar'], 'a']
        rar_cmd.append('-v' + config['rar']['split']) # Splitted volume
        rar_cmd += ['-m1', '-ma5', '-md128m', '-s']
        rar_cmd.append('-rr' + config['rar']['rr']) # Recovery record percentage
        rar_path = os.path.join(backup_path, folder_name + '.rar')
        rar_cmd.append(rar_path)
        rar_cmd.append(full_content_path)
        
        res = execute(rar_cmd, "disk")
        if res != 0:
            print(rar_cmd, "returns", res, file=sys.stderr)
            sys.exit(res)

        # par2 verify
        if int(config['par2']['redundancy']) > 0:

            rar_volume_size = get_size(backup_path)
            block_count = math.ceil(float(rar_volume_size) / int(config['par2']['block']))
            backup_block_count = math.ceil(block_count * int(config['par2']['redundancy']) / 100.0)
            par2_volume_count = math.ceil(backup_block_count / 3.0)

            par2_cmd = [config['toolchain']['par2'], 'c']
            par2_cmd.append('-s' + config['par2']['block']) # block size
            par2_cmd.append('-r' + config['par2']['redundancy']) # redundancy percentage
            par2_cmd.append('-u')
            par2_cmd.append('-m' + config['par2']['memory']) # memory limit
            par2_cmd.append('-v')
            par2_cmd.append('-n' + str(par2_volume_count))
            par2_cmd.append(os.path.join(backup_path, folder_name + '.rar.par2'))
            if os.path.exists(rar_path):
                par2_cmd.append(rar_path)
            else:
                par2_cmd.append(os.path.join(backup_path, folder_name + '.part*.rar'))

            execute(par2_cmd, "disk")
            if res != 0:
                print(par2_cmd, "returns", res, file=sys.stderr)
                sys.exit(res)

        backup_size = get_size(backup_path) / 1024.0 / 1024 / 1024
        max_size = max(full_path_size, backup_size)

        backup_cmd = [config['toolchain']['rclone'], 'copy', backup_path]
        if config['rclone']['raw_account'] == config['rclone']['compress_account']:
            backup_cmd.append(config['rclone']['compress_account'] + ':/' + category_folder + '/' + folder_name + '/backup')
        else:
            backup_cmd.append(config['rclone']['compress_account'] + ':/' + category_folder + '/' + folder_name)
        backup_cmd += ['-v', '--transfers', config['rclone']['threads']]
        backup_cmd += ['--bwlimit', config['rclone']['bandwidth_limit']]

        execute(backup_cmd, "network")
        if res != 0:
            print(backup_cmd, "returns", res, file=sys.stderr)
            sys.exit(res)

        shutil.rmtree(backup_path)

    # rclone upload
    if config['rclone']['raw_account'] != "":
        raw_folder_cmd = [config['toolchain']['rclone'], 'copy', full_content_path]
        raw_folder_cmd.append(config['rclone']['raw_account'] + ':/' + category_folder + '/' + orig_folder_name)
        raw_folder_cmd += ['-v', '--transfers', config['rclone']['threads']]
        raw_folder_cmd += ['--bwlimit', config['rclone']['bandwidth_limit']]
        
        execute(raw_folder_cmd, "network")
        if res != 0:
            print(raw_folder_cmd, "returns", res, file=sys.stderr)
            sys.exit(res)

    print("Quota usage:", max_size, 'GB')


if __name__ == "__main__":
    main() 
    
    


