# -*- coding: utf-8 -*-

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--message', type=str, help='Message to print')
    parser.add_argument('--output', type=str, help='Path to output file')
    args = parser.parse_args()
    message = args.message or "Hello from docker container!"
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(message)
            f.close()
        print(f"Message written to {args.output}")
    else:
        print(message)
