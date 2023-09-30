import subprocess
import argparse

parser = argparse.ArgumentParser(description='credentials')
parser.add_argument('aws_access_key_id', type=str, help='aws_access_key_id')
parser.add_argument('aws_secret_access_key', type=str, help='aws_secret_access_key')
args = parser.parse_args()

command = ['python', '/home/ubuntu/clean-data.py', args.aws_access_key_id, args.aws_secret_access_key]
process = subprocess.Popen(command)
code = process.wait()
print(code) # 0