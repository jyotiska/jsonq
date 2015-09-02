import sys
import argparse
try:
    import simplejson as json
except ImportError:
    import json

parser = argparse.ArgumentParser()
parser.add_argument("-o", "--output", help="Output Format")
parser.add_argument("-f", "--filters", help="Filters (can be comma separated)")
parser.add_argument("-c", "--conditions", help="Condition on filters")
parser.add_argument("-i", "--inputfile", help="Input File")
args = parser.parse_args()

if args.inputfile:
    mode = "file"
    filename = args.inputfile
else:
    mode = "stdin"
    filename = None

filters = []
conditions = {}
condition_flag = False

if args.filters:
    filters = args.filters.split(",")

if args.conditions:
    all_conditions = args.conditions.split(",")
    for each_condition in all_conditions:
        symbol = ""
        if ">" in each_condition:
            split_conditions = each_condition.split(">")
            symbol = ">"
        elif "<" in each_condition:
            split_conditions = each_condition.split("<")
            symbol = "<"
        elif "=" in each_condition:
            split_conditions = each_condition.split("=")
            symbol = "="
        conditions[split_conditions[0]] = '#!#'.join([symbol, split_conditions[-1]])

def read_file_generator(mode, filename):
    if mode == "stdin":
        for line in sys.stdin.readlines():
            yield line
    elif mode == "file":
        with open(filename) as infile:
            for line in infile:
                yield line

for line in read_file_generator(mode, filename):
    if args.conditions:
        condition_flag = False
    else:
        condition_flag = True
    if line.strip():
        line_json = json.loads(line.strip())
        constructed_json = {}
        if len(filters) > 0:
            for each_filter in filters:
                if each_filter in line_json:
                    if each_filter in conditions:
                        if conditions[each_filter].split("#!#")[0] == ">":
                            if float(line_json[each_filter]) > float(conditions[each_filter].split("#!#")[1]):
                                constructed_json[each_filter] = line_json[each_filter]
                                condition_flag = True
                            else:
                                pass
                        elif conditions[each_filter].split("#!#")[0] == "<":
                            if float(line_json[each_filter]) < float(conditions[each_filter].split("#!#")[1]):
                                constructed_json[each_filter] = line_json[each_filter]
                                condition_flag = True
                            else:
                                pass
                        elif conditions[each_filter].split("#!#")[0] == "=":
                            if str(line_json[each_filter]) == str(conditions[each_filter].split("#!#")[1]):
                                constructed_json[each_filter] = line_json[each_filter]
                                condition_flag = True
                            else:
                                pass
                    else:
                         constructed_json[each_filter] = line_json[each_filter]
        else:
            constructed_json = line_json

        if len(constructed_json) > 0 and condition_flag:
            if args.output:
                if args.output == "csv":
                    try:
                        print ','.join(str(each_value) for each_value in constructed_json.values())
                    except IOError:
                        pass
                elif args.output == "tsv":
                    try:
                        print "\t".join(str(each_value) for each_value in constructed_json.values())
                    except IOError:
                        pass
            else:
                try:
                    print json.dumps(constructed_json)
                except IOError:
                    pass
