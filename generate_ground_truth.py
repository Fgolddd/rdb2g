import pandas as pd
import os
from dataloader import SpiderDataLoader
from schema_parser import parse_schema_org
from vector_store import OntologyVectorStore
from agents import MultiAgentSystem
from dotenv import load_dotenv

load_dotenv()

def generate_draft(spider_dir, schema_file, target_databases):
    # 初始化系统
    print("正在初始化系统...")
    kg_store = OntologyVectorStore()
    if not os.path.exists("./data/chroma_db"):
        print("构建向量索引中...")
        terms = parse_schema_org(schema_file)
        kg_store.create_or_load_index(terms)
    else:
        kg_store.create_or_load_index()
    
    agent_system = MultiAgentSystem(kg_store)
    
    draft_data = []

    for db_name in target_databases:
        db_path = os.path.join(spider_dir, db_name, f"{db_name}.sqlite")
        print(f"\n========================================")
        print(f"正在处理数据库: {db_name}")
        print(f"========================================")
        
        try:
            loader = SpiderDataLoader(db_path)
        except FileNotFoundError:
            print(f"❌ 找不到文件: {db_path}")
            continue

        tables = loader.get_all_table_names()
        
        for table in tables:
            print(f"\n>>> 分析表: {table}")
            fingerprint = loader.generate_table_fingerprint(table)
            
            # --- 关键修改：运行完整的智能体流水线 ---
            # 1. 初步映射
            raw_mapping = agent_system.run_mapping_agent(fingerprint)
            
            # 2. 关系识别 (Validator 需要用到主外键信息来判断是否该映射为对象属性)
            relations = agent_system.run_relation_agent(fingerprint)
            
            # 3. 验证与修正 (Validator 会修复 Class vs Property 的错误)
            final_mapping = agent_system.run_validator_agent(fingerprint, raw_mapping, relations)
            
            print(f"    (优化前: {len(raw_mapping)} -> 优化后: {len(final_mapping)} 映射项)")

            # 遍历每一列，记录下来
            for col in fingerprint['columns']:
                col_name = col['name']
                
                # 优先使用 Validator 修正后的结果
                predicted_uri = final_mapping.get(col_name)
                
                # 如果 Validator 把它删了（认为不该映射），回退查看 raw_mapping 或留空
                if not predicted_uri:
                    predicted_uri = raw_mapping.get(col_name, "")
                
                # 简单清洗：确保没有多余的空格
                if predicted_uri:
                    predicted_uri = predicted_uri.strip()

                draft_data.append({
                    "database": db_name,
                    "table": table,
                    "column": col_name,
                    "expected_uri": predicted_uri, # 这里填入的是经过 Validator 优化过的高质量预测
                    "prediction_confidence": "Draft_Auto_Optimized"
                })
        
        loader.close()
    
    # 保存为 CSV
    df = pd.DataFrame(draft_data)


    base_filename = "_".join(target_databases)
    output_file = f"{base_filename}.csv"


    df.to_csv(output_file, index=False)
    print(f"\n✅ 草稿已生成: {output_file}")
    print("请打开 CSV 文件，人工检查 'expected_uri' 列，修正错误的映射。")

if __name__ == "__main__":
    SPIDER_DIR = "data/spider_data/database"
    SCHEMA_FILE = "data/schemaorg.jsonld"
    
    # 这里填入您想评估的几个数据库名字
    # 比如 Spider 数据集里的这几个
    TARGET_DBS = ["cinema"] 
    
    generate_draft(SPIDER_DIR, SCHEMA_FILE, TARGET_DBS)