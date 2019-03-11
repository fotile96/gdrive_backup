import subprocess
import sys
import configparser

def get_abspath(exec_name):
    """ Get absolute path for exec file, return none if not found """
    proc = subprocess.Popen(['which', exec_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = proc.communicate()
    bin_path = outs.decode(sys.stdout.encoding).rstrip('\n')
    if len(bin_path) > 0:
        return bin_path
    else:
        return None


def main():
    config = configparser.ConfigParser()
    
    # Get toolchain
    print("** Start checking toolchain **")
    rclone_path = get_abspath('rclone')
    if rclone_path is None:
        print("Rclone should be installed first")
        raise FileNotFoundError
    else:
        print("Found rclone at:", rclone_path)

    rar_path = get_abspath('rar')
    if rar_path is None:
        print('RAR should be installed first')
        raise FileNotFoundError
    else:
        print("Found RAR at:", rar_path)

    par2_path = get_abspath('par2')
    if par2_path is None:
        print('par2 should be installed first')
        raise FileNotFoundError
    else:
        print("Found par2 at:", par2_path)

    tsp_path = get_abspath('tsp')
    if tsp_path is None:
        print('tsp should be installed first')
        raise FileNotFoundError
    else:
        print("Found tsp at:", tsp_path)

    config['toolchain'] = {'rclone': rclone_path, "rar": rar_path, "par2": par2_path, "tsp": tsp_path}
    print("** Toolchain check finish **")

    # Get raw folder upload account
    raw_upload = input("Please input rclone remote for raw folder upload: ")
    compress_upload = input("Please input rclone remote for compressed upload: ")
    upload_threads = input("Please input the number of concurrent rclone upload threads: ")
    bandwidth_limit = input("Please input the bandwidth limit for rclone upload: ")

    config['rclone'] = {'raw_account': raw_upload, 'compress_account': compress_upload, 'threads': upload_threads, 'bandwidth_limit': bandwidth_limit}

    # Get rar config
    split = input("Please input default size for rar splitted volume, default is \"4g\": ")
    recover = input('Please input RR percentage, e.g. 3: ')
    config['rar'] = {'split': split, 'rr': recover + 'p'}

    # Get par2 config
    bs_input = input('Please input block size, e.g. 1g, 512m, 512k: ')
    if bs_input[-1] == 'g':
        block_size = int(bs_input[:-1]) * 1024 * 1024 * 1024
    elif bs_input[-1] == 'm':
        block_size = int(bs_input[:-1]) * 1024 * 1024
    elif bs_input[-1] == 'k':
        block_size = int(bs_input[:-1]) * 1024

    redundancy = input('Please input redundency percentage: ')
    memory = input('Please input memory limit, in megabytes: ')
    
    config['par2'] = {'block': block_size, 'redundancy': redundancy, 'memory': memory}

    # Get misc config
    prefix = input("Please input the location to store archives temporarily: ")
    config['misc'] = {'prefix': prefix}

    with open('./config.ini', 'w') as f:
        config.write(f)

if __name__ == "__main__":
    main() 
    


