#!/usr/bin/env python3

import argparse
import random

columns = {
    'temperature': {
        'type': 'int',
        'range': [-5, 25]
    },
    'country': {
        'type': 'array',
        'values': [
            'Belgium',
            'France',
            'Luxembourg',
            'Netherlands',
            'Germany'
        ]
    },
    'weather': {
        'type': 'array',
        'values': [
            'Rainy',
            'Sunny',
            'Cloudy'
        ]
    }
}

def gen_header():
    return [key for key in columns.keys()]

def gen_entry():
    entry = []
    for key in columns.keys():
        column = columns[key]
        if column['type'] == 'int':
            [a, b] = column['range']
            value = random.uniform(a, b)
            value = int(value)
            entry.append(str(value))
        if column['type'] == 'array':
            value = random.choice(column['values'])
            entry.append(value)
    return entry

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--n', help='Number of record (default: 10')
    parser.add_argument('--separator', help='Separator (default: ";")')
    args = parser.parse_args()

    n = 10
    separator = ';'

    if args.n:
        n = int(args.n)
    if args.separator:
        separator = args.separator

    with open('../dataset/data.csv', 'w') as file:
        file.write(separator.join(gen_header()) + '\n')
        for _ in range(n):
            file.write(separator.join(gen_entry()) + '\n')
        file.close()