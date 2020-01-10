import argparse
import subprocess
import os
import rw_config


def main():
    args = rw_config.read_config("run_args.json")["export"]
    repo_name = args["repo_name"]
    target_dir = args["target_dir"]
    git_file_path = args["git_file_path"]
    commit = args["commit_code"]
    src_system, TGT_ENV = parse_args()
    file_list = get_files_names(src_system, TGT_ENV)

    download(repo_name, git_file_path, target_dir, commit, file_list)
    namenode = get_namenode()
    rw_config.extract_config_details(src_system, file_list, namenode)


class cd:
    """Context manager for changing the current working directory"""

    def __init__(self, newPath):
        self.newPath = newPath

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)


def shcmd(cmd, ignore_error=False):
    ret = subprocess.call(cmd, shell=True)
    print('Returned', ret)
    if ignore_error == False and ret != 0:
        raise RuntimeError("Failed to execute {}. Return code:{}".format(
            cmd, ret))
    return ret


def download(repo_name, git_file_path, target_dir, commit, file_list):
    token = os.getenv("GITHUB_TOKEN")
    if token is None:
        raise RuntimeError("Environment Var GITHUB_TOKEN does not exist")

    for file in file_list:
        cmd = "curl -H 'Authorization: token {token}' " \
              "-H 'Accept: application/vnd.github.v3.raw' -o {file} " \
              "-L https://raw.githubusercontent.com/NLDJAI1/" \
              "{repo_name}/{commit}/{git_file_path}/{file}".format(
            token=token,
            repo_name=repo_name,
            commit=commit,
            git_file_path=git_file_path,
            file=file
        )

        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)

        with cd(target_dir):
            shcmd(cmd)


def get_namenode():
    out = subprocess.Popen(['hdfs', 'getconf', '-namenodes'],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()
    namenode = stdout.decode("utf-8").rstrip()
    return namenode


def parse_args():
    parser = argparse.ArgumentParser(
        description=""
    )
    parser.add_argument('--src_system', action='store', help="USe --src_system to specify the source system")
    parser.add_argument('--TGT_ENV', action='store', help="USe --src_system to specify the source system")
    args = parser.parse_args()
    kargs = vars(args)
    if not all(v for _, v in kargs.items()):
        parser.print_help()
        exit(1)

    src_system = kargs.get("src_system")
    TGT_ENV = kargs.get("TGT_ENV")
    return src_system, TGT_ENV


"""
    parser.add_argument('--TGT_ENV', action='store', help="USe --src_system to specify the source system")
    args = parser.parse_args()
    kargs = vars(args)
    if not all(v for _, v in kargs.items()):
        parser.print_help()
        exit(1)

    TGT_ENV = kargs.get("TGT_ENV")
    return TGT_ENV
"""


def get_files_names(src_system, TGT_ENV):
    file_list = []
    if src_system == 'SYM':
        file_list = ['Symphony.json', TGT_ENV + ".json"]
    elif src_system == 'CYC':
        file_list = ['CyC_SAS.json', TGT_ENV + ".json"]
    elif src_system == 'SYMR':
        file_list = ['SymphonyRRead.json', TGT_ENV + ".json"]
    elif src_system == 'LCT':
        file_list = ['Large_Case_Tool.json', TGT_ENV + ".json"]
    elif src_system == 'ORF':
        file_list = ['Oracle_Financials.json', TGT_ENV + ".json"]
    return file_list


if __name__ == '__main__':
    main()
