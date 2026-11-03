"""Bounded LLM-FK-inspired singleton adaptation, not official reproduction."""
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
from experiments.public_kg.artifacts import file_hash, write_json, materialize
from experiments.public_kg.methods import schema_profile
from experiments.public_kg.relation_component_baselines import _terms, _class_hierarchy, _ind_candidates

BASE=Path('data/public/benchmark_v2')
OUT=Path('data/eval/public_kg/frontier_20261030/llmfk_retry')

def execute():
    load_dotenv()
    client=OpenAI(api_key=os.environ['DASHSCOPE_API_KEY'],base_url=os.getenv('QWEN_BASE_URL'),timeout=120,max_retries=0)
    total=0
    for dataset in ('foshan_osm','geonames_cn'):
        folder=OUT/dataset
        if folder.exists():
            raise FileExistsError(folder)
        folder.mkdir(parents=True)
        db=BASE/dataset/'sample.sqlite'
        ontology=BASE/'ontology/target_terms.json'
        cp=Path('data/eval/public_kg/short_plan_20261029')/f'{dataset}_full_r1/predictions.json'
        context=json.loads(cp.read_text())
        profile=schema_profile(db)
        classes={x['table']:x['target_uri'] for x in context['table_mappings']}
        candidates,evidence=_ind_candidates(db,profile,classes,_terms(ontology),_class_hierarchy(ontology))
        # Restrict approximate IND candidates to exact singleton keys and inclusion.
        candidates=[r for r,e in zip(candidates,evidence) if e['source_hit_rate']==1 and e['target_uniqueness']==1 and r['source_table']!=r['target_table']]
        write_json(folder/'candidates.json',candidates)
        state={'candidates':[dict(id=i,rule=r) for i,r in enumerate(candidates)],'schema':profile}
        manifest={'dataset':dataset,'method':'LLM-FK-inspired-singleton','status':'started','paper':'https://arxiv.org/abs/2603.07278','dataset_sha256':file_hash(db),'ontology_sha256':file_hash(ontology),'context_sha256':file_hash(cp),'source_sha256':file_hash(Path(__file__)),'deviations':['singleton exact IND only; no composite MinUCC search','ontology-class pruning precedes profiler','one batched request per reasoning stage; no original prompts or DeepSeek-R1','candidate graph conflict/cycle analysis delegated to verifier; no separate deterministic cycle solver'],'candidate_count':len(candidates),'model':os.getenv('QWEN_CHAT_MODEL','qwen-plus')}
        write_json(folder/'manifest.json',manifest)
        trace=[]
        try:
            selected=list(range(len(candidates)))
            for role in ('Profiler','Interpreter','Refiner','Verifier'):
                prompt=json.dumps({'role':role,'instruction':'Assess missing foreign-key candidates. Return JSON with selected_ids (candidate ids to retain) and reasoning. Profiler uses keys/statistics; Interpreter uses domain meanings; Refiner assesses semantic/syntactic/statistical validity; Verifier resolves conflicting targets and cycles. Never invent ids. All candidate ids remain available; prior_selected_ids is advisory.','prior_selected_ids':selected,**state},ensure_ascii=False)
                if total+len(prompt.encode())+5000>300000:
                    raise RuntimeError('cumulative reserved token cap')
                prompt += "\nKeep reasoning below 100 words; return selected_ids as integer array only, no per-candidate explanations."
                event={'role':role,'request':prompt}
                trace.append(event)
                response=client.chat.completions.create(model=manifest['model'],messages=[{'role':'user','content':prompt}],max_tokens=5000,temperature=0,response_format={'type':'json_object'},extra_body={'enable_thinking':False})
                if response.usage is None:
                    raise RuntimeError('missing provider usage')
                total+=response.usage.total_tokens
                event.update(response=response.choices[0].message.content,usage=response.usage.model_dump())
                if response.choices[0].finish_reason=='length':
                    raise ValueError('truncated response')
                raw=json.loads(event['response'])
                selected=raw['selected_ids']
                if not isinstance(selected,list) or any(type(i)!=int or not 0<=i<len(candidates) for i in selected):
                    raise ValueError('invalid candidate ids')
                state['previous_reasoning']=raw.get('reasoning')
                write_json(folder/'trace.json',trace)
            prediction={'run_status':'success','table_mappings':context['table_mappings'],'mappings':[{'table':t,'column':c['name'],'target_uri':None} for t,v in profile.items() for c in v['columns']],'relation_rules':[candidates[i] for i in sorted(set(selected))]}
            write_json(folder/'predictions.json',prediction)
            manifest['prediction_sha256']=file_hash(folder/'predictions.json')
            manifest['status']='success'
            manifest['graph']=materialize(db,folder/'predictions.json',ontology,folder/'graph')
        except Exception as exc:
            manifest.update(status='failed',error_type=type(exc).__name__)
            trace.append({'error_type':type(exc).__name__})
        write_json(folder/'trace.json',trace)
        manifest['known_total_tokens_cumulative']=total
        write_json(folder/'manifest.json',manifest)
        print(json.dumps(manifest),flush=True)
        if manifest['status']=='failed':
            break

if __name__=='__main__':
    execute()
