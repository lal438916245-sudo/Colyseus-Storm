"""Shared report, audit, artifact integrity and exit-code contract."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import html
import json
import os
import base64


def save(path, value):
    path=Path(path)
    temporary=path.with_name(path.name+'.tmp')
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
    os.replace(temporary,path)


def decide(summary, audit_ok, pipeline_errors=()):
    stages=summary.get('stages',[])
    invalid=(pipeline_errors or not audit_ok or not summary.get('cleanup',{}).get('ok')
             or summary.get('fatal_error') or summary.get('guard_reasons') or not stages
             or any(not r.get('generator_valid') for r in stages))
    if invalid:return 3
    if any(not r.get('passed') for r in stages):return 2
    return 0 if summary.get('complete_planned_steps') else 3


def render_report(folder, summary):
    esc=lambda value:html.escape(str(value))
    gate=summary.get('gate',{})
    lines=['<!doctype html><html lang="zh-CN"><meta charset="utf-8">',
      '<meta name="viewport" content="width=device-width,initial-scale=1">',
      '<title>Colyseus-Storm 运行报告</title><style>body{font:16px/1.7 system-ui;max-width:1100px;margin:40px auto;padding:0 20px;color:#203045}table{border-collapse:collapse;width:100%}td,th{padding:9px;border-bottom:1px solid #ddd;text-align:left}img{max-width:100%}code{word-break:break-all}.status{font-size:22px;font-weight:bold}a{color:#245b9e}</style>',
      '<h1>Colyseus-Storm 运行报告</h1>',
      f'<p class="status">{esc(gate.get("status","incomplete"))} · Exit {esc(gate.get("exit_code",3))}</p>',
      f'<p>运行 {esc(summary.get("run_id",folder.name))} · 模式 {esc(summary.get("mode","verify"))}</p>',
      '<p>0：验收目标通过；2：有效数据证明业务目标未达标；3：执行、发压、审计或清理不完整。</p>',
      f'<p>审计：{esc(summary.get("audit",{}).get("passed",False))}；清理：{esc(summary.get("cleanup",{}).get("ok",False))}</p>']
    if summary.get('mode')=='search':
        lines.append(f'<p>搜索状态：{esc(summary.get("search_status"))}；确认通过人数：{esc(summary.get("confirmed_capacity"))}；有效失败上界：{esc(summary.get("failed_upper"))}</p>')
    lines+=['<table><thead><tr><th>人数</th><th>实际在线</th><th>观察秒数</th><th>业务更新/s</th><th>ACK覆盖率</th><th>RTT p95/ms</th><th>RTT p99/ms</th><th>判定</th></tr></thead><tbody>']
    for row in summary.get('stages',[]):
        cells=[row.get(k,'—') for k in ['target','live','seconds','applied_per_second','ack_ratio','rtt_p95_ms','rtt_p99_ms','passed']]
        lines.append('<tr>'+''.join('<td>'+esc(round(c,3) if isinstance(c,float) else c)+'</td>' for c in cells)+'</tr>')
    lines+=['</tbody></table>']
    if (folder/'staircase.png').exists():
        encoded=base64.b64encode((folder/'staircase.png').read_bytes()).decode('ascii')
        lines.append('<h2>测量趋势</h2><img src="data:image/png;base64,'+encoded+'" alt="测量趋势">')
    problems=list(summary.get('audit',{}).get('failures',[]))
    problems+=['人数 '+str(r.get('target'))+': '+', '.join(r.get('failed_criteria',[])+r.get('generator_issues',[])) for r in summary.get('stages',[]) if not r.get('passed')]
    if problems:lines.append('<h2>未通过原因</h2><pre>'+esc('\n'.join(problems))+'</pre>')
    if summary.get('fatal_error'):lines.append('<h2>执行异常</h2><pre>'+esc(json.dumps(summary['fatal_error'],ensure_ascii=False,indent=2))+'</pre>')
    if summary.get('pipeline_errors'):lines.append('<h2>处理异常</h2><pre>'+esc('\n'.join(summary['pipeline_errors']))+'</pre>')
    lines.append('<h2>证据与产物</h2><ul>')
    for path in sorted(folder.iterdir()):
        if path.is_file() and path.name!='report.html' and not path.name.startswith('.'):
            lines.append(f'<li><a href="{esc(path.name)}">{esc(path.name)}</a></li>')
    lines+=['</ul><p>同机、指定工作负载与观察窗口下的结果，不代表理论或线上极限。CPU 100% 表示一个核心的等效用量。</p></html>']
    (folder/'report.html').write_text('\n'.join(lines),encoding='utf-8')


def seal(folder, audit):
    files=[p for p in sorted(folder.rglob('*')) if p.is_file()
           and p not in (folder/'audit.json',folder/'ARTIFACTS.sha256.json') and not p.name.endswith('.tmp')]
    audit['artifact_sha256']={p.relative_to(folder).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    save(folder/'audit.json',audit)
    files.append(folder/'audit.json')
    save(folder/'ARTIFACTS.sha256.json',{p.relative_to(folder).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files})


def finish(folder, summary, audit, errors=(), exit_code=None):
    code=decide(summary,audit.get('passed'),errors) if exit_code is None else exit_code
    summary.update(schema='storm/1.0',audit={k:v for k,v in audit.items() if k!='artifact_sha256'},pipeline_errors=list(errors),
        gate={'exit_code':code,'status':{0:'passed',2:'slo_failed',3:'inconclusive'}[code]},
        finalized_at=datetime.now(timezone.utc).isoformat())
    save(folder/'summary.json',summary)
    save(folder/'audit.json',audit)
    render_report(folder,summary)
    seal(folder,audit)
    print('STORM_RESULT='+json.dumps({'output':str(folder),'exit_code':code,'status':summary['gate']['status']},ensure_ascii=False),flush=True)
    return code


def finalize(folder):
    folder=Path(folder);errors=[]
    summary=json.loads((folder/'summary.json').read_text(encoding='utf-8'))
    try:
        from analyze_step_results import audit as audit_run
        audit=audit_run(folder)
        # Independently reject stale/tampered per-level decisions.
        from step_load import evaluate
        manifest=json.loads((folder/'manifest.json').read_text(encoding='utf-8'))
        limits=manifest['design']
        for row in summary.get('stages',[]):
            result=evaluate(row,limits['p95_rtt_limit_ms'],limits['p99_rtt_limit_ms'])
            if any(row.get(key)!=value for key,value in result.items()):
                audit['passed']=False;audit['failures'].append('SLO decision mismatch')
    except Exception as exc:
        errors.append('audit: '+str(exc));audit={'passed':False,'failures':[str(exc)]}
    try:
        if summary.get('stages'):
            from plot_step_results import plot
            plot(folder)
    except Exception as exc:errors.append('plot: '+str(exc))
    return finish(folder,summary,audit,errors)


def failure_report(folder, message):
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    summary={'run_id':folder.name,'mode':'verify','stages':[],'complete_planned_steps':False,
             'fatal_error':{'message':message},'cleanup':{'ok':False}}
    if (folder/'summary.json').exists():summary=json.loads((folder/'summary.json').read_text(encoding='utf-8'))
    return finish(folder,summary,{'passed':False,'failures':[message]},[message],3)
