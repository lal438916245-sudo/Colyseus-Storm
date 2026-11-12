"""Render measured staircase trends; no network or live-process interaction."""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def plot(folder, additional=()):
    summary=json.loads((folder/'summary.json').read_text(encoding='utf-8'))
    rows=summary['stages'];groups={}
    for run in [folder,*additional]:
        data=json.loads((run/'summary.json').read_text(encoding='utf-8'))
        manifest=json.loads((run/'manifest.json').read_text(encoding='utf-8'))
        key=(manifest['parameters']['hold'],manifest['parameters']['workers'])
        groups.setdefault(key,[]).extend(data['stages'])
    rows=[row for group in groups.values() for row in group]
    if not rows:raise ValueError('No completed measurement levels')
    n=sorted({r['target'] for r in rows})
    limits=json.loads((folder/'manifest.json').read_text(encoding='utf-8')).get('design',{})
    p95_limit=limits.get('p95_rtt_limit_ms',200);p99_limit=limits.get('p99_rtt_limit_ms',500)
    fig,axes=plt.subplots(3,1,figsize=(11,10),constrained_layout=True)
    fig.suptitle('Colyseus-Storm | Measured staircase',fontsize=19,fontweight='bold')
    palette=['#245B9E','#BD4356','#29957B','#BF6A25']
    for i,((hold,workers),group) in enumerate(groups.items()):
        group.sort(key=lambda r:r['target']);x=[r['target'] for r in group]
        label=f'{hold:g}s / {workers} load workers';marker='*' if hold>=300 else 'o'
        color=palette[i%len(palette)];style='-' if i==0 else 'None'
        axes[0].plot(x,[r['applied_per_second'] for r in group],marker=marker,linestyle=style,color=color,label=label,markersize=9 if hold>=300 else 5)
        axes[1].plot(x,[r['rtt_p95_ms'] for r in group],marker=marker,linestyle=style,color=color,label=label,markersize=9 if hold>=300 else 5)
        axes[1].plot(x,[r['rtt_p99_ms'] for r in group],marker=marker,linestyle=style,color=color,alpha=.4,markerfacecolor='none')
        axes[2].plot(x,[r['server_cpu_mean'] for r in group],marker=marker,linestyle=style,color=color,label=label,markersize=9 if hold>=300 else 5)
    axes[0].plot(n,[v*10 for v in n],'--',color='#9099A5',label='Offered target: 10 updates/s/player')
    axes[0].set_ylabel('Business updates/s');axes[0].legend(loc='upper left')
    axes[1].set_title('Filled markers: application RTT p95 | Light/open markers: p99',fontsize=10,loc='left')
    axes[1].axhline(p95_limit,color='#BF6A25',linestyle='--',alpha=.7,label=f'p95 limit: {p95_limit:g} ms')
    axes[1].axhline(p99_limit,color='#BD4356',linestyle=':',alpha=.7,label=f'p99 limit: {p99_limit:g} ms')
    axes[1].set_yscale('log');axes[1].set_ylabel('Business RTT (ms, log scale)');axes[1].legend(loc='upper left',ncol=2,fontsize=9)
    axes[2].scatter([r['target'] for r in rows],[r['worker_cpu_p95'] for r in rows],marker='s',color='#888F99',alpha=.6,label='Load-worker CPU p95')
    axes[2].axhline(100,color='#BD4356',linestyle='--',alpha=.6,label='One CPU equivalent')
    axes[2].set_ylabel('CPU % (100% = one core)');axes[2].set_xlabel('Actual concurrent players');axes[2].legend(loc='upper left',fontsize=9)
    for axis in axes:
        axis.grid(alpha=.18);axis.spines[['top','right']].set_visible(False)
        for r in rows:
            if not r['passed']:axis.axvline(r['target'],color='#BD4356',alpha=.2,linewidth=8)
    fig.savefig(folder/'staircase.png',dpi=160)
    plt.close(fig)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('run_dir',type=Path)
    p.add_argument('--additional',type=Path,nargs='*',default=[])
    a=p.parse_args();plot(a.run_dir,a.additional)
