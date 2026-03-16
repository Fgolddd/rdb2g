import json


def _extract_refs(field_data):
    """处理嵌套的 @id 引用"""
    if not field_data:
        return "None"
    if isinstance(field_data, list):
        ids = [item.get('@id') for item in field_data if isinstance(item, dict) and '@id' in item]
        return ", ".join(ids) if ids else "None"
    if isinstance(field_data, dict):
        return field_data.get('@id', "None")
    return str(field_data)


def _normalize_term(node):
    """将不同来源的知识条目统一为内部结构"""
    uri = node.get('uri') or node.get('id') or node.get('@id')
    if not uri:
        return None

    label = node.get('label') or node.get('name') or node.get('rdfs:label')
    if isinstance(label, dict):
        label = label.get('@value')

    comment = node.get('comment') or node.get('description') or node.get('desc') or node.get('rdfs:comment')
    if isinstance(comment, dict):
        comment = comment.get('@value')

    domain = (
        node.get('domain')
        or node.get('domainIncludes')
        or node.get('schema:domainIncludes')
    )
    range_val = (
        node.get('range')
        or node.get('rangeIncludes')
        or node.get('schema:rangeIncludes')
    )

    term_dict = {
        'uri': str(uri),
        'type': str(node.get('type') or node.get('@type') or "Thing"),
        'label': str(label) if label else str(uri),
        'comment': str(comment) if comment else "No description.",
        'domain': _extract_refs(domain),
        'range': _extract_refs(range_val),
    }
    return term_dict

def parse_schema_org(file_path):
    """解析 JSON-LD 文件，提取 URI, Label, Comment, Domain, Range"""
    print(f"正在解析本体文件: {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    graph = data.get('@graph', [])
    parsed_terms = []

    for node in graph:
        term_dict = _normalize_term(node)
        if term_dict:
            parsed_terms.append(term_dict)

    print(f"解析完成，共提取 {len(parsed_terms)} 个术语。")
    return parsed_terms


def parse_private_kb(file_path):
    """解析私域知识库（JSON），并统一为标准术语结构"""
    print(f"正在解析私域知识库: {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if isinstance(data, list):
        entries = data
    elif isinstance(data, dict):
        if isinstance(data.get('terms'), list):
            entries = data['terms']
        elif isinstance(data.get('@graph'), list):
            entries = data['@graph']
        else:
            entries = [data]
    else:
        raise ValueError("私域知识库格式不支持：仅支持 JSON 对象或对象数组。")

    parsed_terms = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        term_dict = _normalize_term(entry)
        if term_dict:
            parsed_terms.append(term_dict)

    print(f"私域知识库解析完成，共提取 {len(parsed_terms)} 个术语。")
    return parsed_terms