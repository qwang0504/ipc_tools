import pandas as pd
import matplotlib.pyplot as plt  
import re
from typing import List, Dict, Optional
from matplotlib import collections  as mc

def parse_logs(filename: str) -> List[Dict]:

    log_entry = re.compile(r"""
        (?P<datetime>\d+-\d+-\d+ \s+ \d+:\d+:\d+,\d+) \s+
        (?P<process_id>Process-\d+) \s+
        (?P<process_name>(\w|\.)+) \s+
        (?P<loglevel>\w+) \s+
        (?P<operation>\w+) ,\s+
        (?P<timestamp_start>\d+\.\d+) ,\s+
        (?P<timestamp_stop>\d+\.\d+)
        """, re.VERBOSE)
    
    with open(filename, 'r') as f:
        content = f.read()
        entries = [e.groupdict() for e in log_entry.finditer(content)]

    return entries


def plot_logs(filename: str) -> None:
    
    # get entries using regexp
    entries = parse_logs(filename)

    # convert entries to pandas dataframe
    data = pd.DataFrame(entries)
    data = data.astype({
        'datetime': 'datetime64[ns]',
        'process_id': 'str',
        'process_name': 'str',
        'loglevel': 'str',
        'operation': 'str',
        'timestamp_start': 'float64',
        'timestamp_stop': 'float64'
    })

    # shift absolute tiimings to start at zero
    min_timestamp = min(data['timestamp_start'])
    data['timestamp_start'] = data['timestamp_start'] - min_timestamp
    data['timestamp_stop'] = data['timestamp_stop'] - min_timestamp

    # plot
    y = 0
    col = {}
    col['get'] = 'g'
    col['put'] = 'r'
    queues = data['process_name'].unique()
    operations =  data['operation'].unique()
    yticks = []
    fig, ax = plt.subplots()
    for queue in queues:
        y += 1
        yticks.append(y)
        for operation in operations: 
            df = data[(data['process_name'] == queue) & (data['operation'] == operation)]
            lines = [[(x0,y),(x1,y)] for x0,x1 in zip(df['timestamp_start'], df['timestamp_stop'])]
            lc = mc.LineCollection(lines, colors=col[operation])
            ax.add_collection(lc)
    ax.autoscale()
    ax.set_yticks(yticks, queues)
    plt.show()
