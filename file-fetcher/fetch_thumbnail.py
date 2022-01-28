import sys
import argparse
from md5s3stash import md5s3stash

def main(**kwargs):
    stasher = md5s3stash(**kwargs)
    stasher.stash()

    print(f"{stasher.md5hash=} {stasher.mime_type=} {stasher.dimensions=}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    required = parser.add_mutually_exclusive_group(required=True)
    required.add_argument('--url')
    required.add_argument('--localpath')
    args = parser.parse_args()
    if args.url:
        kwargs = {'url': args.url}
    else:
        kwargs = {'localpath': args.localpath}

    sys.exit(main(**kwargs))