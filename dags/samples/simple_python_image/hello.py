# -*- coding: utf-8 -*-

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--message', type=str, help='Message to print')
    args = parser.parse_args()
    message = args.message or "Hello from docker container!"
    print(message)
