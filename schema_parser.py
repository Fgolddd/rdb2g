import json

def parse_schema_org(file_path):
    """解析 JSON-LD 文件，提取 URI, Label, Comment, Domain, Range"""
    print(f"正在解析本体文件: {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    graph = data.get('@graph', [])
    parsed_terms = []

    def extract_refs(field_data):
        """处理嵌套的 @id 引用"""
        if not field_data: return "None"
        if isinstance(field_data, list):
            ids = [item.get('@id') for item in field_data if isinstance(item, dict) and '@id' in item]
            return ", ".join(ids)
        elif isinstance(field_data, dict):
            return field_data.get('@id', "None")
        return str(field_data)

    for node in graph:
        uri = node.get('@id')
        if not uri: continue

        # 提取并清洗字段
        label = node.get('rdfs:label')
        if isinstance(label, dict): label = label.get('@value')
        
        comment = node.get('rdfs:comment')
        if isinstance(comment, dict): comment = comment.get('@value')

        domain = extract_refs(node.get('schema:domainIncludes'))
        range_val = extract_refs(node.get('schema:rangeIncludes'))

        term_dict = {
            'uri': uri,
            'type': str(node.get('@type')),
            'label': str(label) if label else uri,
            'comment': str(comment) if comment else "No description.",
            'domain': domain,
            'range': range_val
        }
        parsed_terms.append(term_dict)

    print(f"解析完成，共提取 {len(parsed_terms)} 个术语。")
    return parsed_terms