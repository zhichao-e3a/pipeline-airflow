import argparse
import subprocess

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--origin', required=True, choices=['rec', 'hist'])
    parser.add_argument('--limit', required=False)
    origin = parser.parse_args().origin
    limit  = int(parser.parse_args().limit) if parser.parse_args().limit else None

    if limit is not None:
        subprocess.run(['python', '-m', 'tests.query', '--origin', origin, '--limit', str(limit)], check=True)
    else:
        subprocess.run(['python', '-m', 'tests.query', '--origin', origin], check=True)
    subprocess.run(['python', '-m', 'tests.filter', '--origin', origin], check=True)

if __name__ == '__main__':
    main()