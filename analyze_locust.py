"""Compatibility CSV analysis using the same environment; no pandas dependency."""
import argparse
import csv
from pathlib import Path


def read_csv(path):
    for encoding in ('utf-8-sig','gb18030'):
        try:
            with Path(path).open(encoding=encoding,newline='') as raw:return list(csv.DictReader(raw))
        except UnicodeDecodeError:continue
    raise ValueError('CSV encoding unsupported')


def analyze_locust_data(history_file,stats_file,failures_file,output_dir="reports"):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    output=Path(output_dir);output.mkdir(parents=True,exist_ok=True)
    stats=read_csv(stats_file);failures=read_csv(failures_file)
    history=[r for r in read_csv(history_file) if r.get('Name')=='Aggregated']
    if not stats or not history:raise ValueError('Locust statistics/history are empty')
    # Keep source global percentiles; percentile averages are not global P95.
    with (output/'colyseus_performance_report.csv').open('w',encoding='utf-8-sig',newline='') as raw:
        writer=csv.DictWriter(raw,fieldnames=list(stats[0]));writer.writeheader();writer.writerows(stats)
    times=[float(r['Timestamp']) for r in history];times=[n-times[0] for n in times]
    def number(row,key):
        value=row.get(key)
        # Locust emits N/A before a percentile window has any samples.
        # Leave a gap rather than inventing zero latency.
        if value is None or str(value).strip().lower() in ('','n/a','nan','none'):return float('nan')
        return float(value)
    fig,axes=plt.subplots(2,2,figsize=(11,7),constrained_layout=True)
    for key,label,axis in [('Requests/s','Requests per second',axes[0,0]),('User Count','Locust virtual users',axes[0,1]),
                           ('95%','Locust request P95 (not application RTT)',axes[1,0]),('Failures/s','Failures per second',axes[1,1])]:
        axis.plot(times,[number(r,key) for r in history]);axis.set_title(label);axis.set_xlabel('Seconds');axis.grid(alpha=.2)
    fig.savefig(output/'colyseus_performance_report.png',dpi=140);plt.close(fig)
    return {'request_rows':len(stats),'failure_groups':len(failures),'history_points':len(history)}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-dir',type=Path,default=Path('reports'))
    p.add_argument('--prefix',default='locust');a=p.parse_args()
    print(analyze_locust_data(a.run_dir/(a.prefix+'_stats_history.csv'),a.run_dir/(a.prefix+'_stats.csv'),a.run_dir/(a.prefix+'_failures.csv'),a.run_dir))
