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
        (?P<t_start>\d+\.\d+) ,\s+
        (?P<t_lock_acquired>\d+\.\d+) ,\s+
        (?P<t_lock_released>\d+\.\d+)
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
        't_start': 'float64',
        't_lock_acquired': 'float64',
        't_lock_released': 'float64'
    })

    # shift absolute tiimings to start at zero
    min_timestamp = min(data['t_start'])
    data['t_start'] = data['t_start'] - min_timestamp
    data['t_lock_acquired'] = data['t_lock_acquired'] - min_timestamp
    data['t_lock_released'] = data['t_lock_released'] - min_timestamp

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
            lines_wait = [[(x0,y),(x1,y)] for x0,x1 in zip(df['t_start'], df['t_lock_acquired'])]
            lines_lock = [[(x0,y),(x1,y)] for x0,x1 in zip(df['t_lock_acquired'], df['t_lock_released'])]
            collection_wait = mc.LineCollection(lines_wait, colors='gray')
            collection_lock = mc.LineCollection(lines_lock, colors=col[operation])
            ax.add_collection(collection_wait)
            ax.add_collection(collection_lock)
    ax.autoscale()
    ax.set_yticks(yticks, queues)
    plt.show()
