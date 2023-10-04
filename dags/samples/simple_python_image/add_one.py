# -*- coding: utf-8 -*-

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=int, help='Input number')
    args = parser.parse_args()
    input = args.input
    print(input + 1)
