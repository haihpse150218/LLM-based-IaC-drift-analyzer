import hcl2
import os
import re
import json
import hashlib
from collections import OrderedDict
from dotenv import load_dotenv
from fnmatch import fnmatch

# Load .env file
load_dotenv()

def detect_file_type(file_path):
    """Phase 1: File type detection"""
    ignore_patterns = os.getenv('LIST_IGNORE_FILE', '').split(',')
    ignore_patterns = [pattern.strip() for pattern in ignore_patterns if pattern.strip()]
    
    file_name = os.path.basename(file_path)
    for pattern in ignore_patterns:
        if fnmatch(file_name, pattern):
            print(f"Ignoring file {file_path} due to pattern {pattern}")
            return 'unknown'

    ext = os.path.splitext(file_path)[1].lower()
    if ext not in ['.tf', '.tfvars', '.hcl']:
        return 'unknown'
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read(1000)
            if 'resource' in content or 'module' in content or 'variable' in content or 'provider' in content or 'terraform' in content:
                return 'terraform'
            if ext == '.tfvars':
                return 'tfvars'
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return 'unknown'

def parse_ast(file_path):
    """Phase 2: Attempt AST parse with hcl2"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = hcl2.load(f)
        return config
    except Exception as e:
        print(f"Parse failed for {file_path}: {e}")
        return None

def canonicalize(config):
    """Phase 3: Canonicalize - sort attributes, handle heredoc (basic)"""
    def sort_dict(d):
        if isinstance(d, dict):
            return OrderedDict(sorted((k, sort_dict(v)) for k, v in d.items()))
        elif isinstance(d, list):
            return [sort_dict(item) for item in d]
        return d
    return sort_dict(config)

def resolve_variables(config, tfvars_path=None):
    """Phase 4: Resolve variables best-effort"""
    variables = {}
    if tfvars_path and os.path.exists(tfvars_path):
        try:
            with open(tfvars_path, 'r', encoding='utf-8') as f:
                vars_config = hcl2.load(f)
            variables = vars_config
        except Exception as e:
            print(f"Error parsing tfvars {tfvars_path}: {e}")
    
    def substitute(obj):
        if isinstance(obj, str) and obj.startswith('${var.'):
            var_name = obj.split('.')[1].strip('}')
            if var_name in variables:
                return variables[var_name]
            return obj
        elif isinstance(obj, dict):
            return {k: substitute(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [substitute(item) for item in obj]
        return obj
    
    return substitute(config)

def calculate_lines(file_path, chunk_content, block_type, block_name):
    """Calculate start_line and end_line for a chunk"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            content_str = chunk_content if isinstance(chunk_content, str) else format_content(chunk_content, block_type, block_name)
        
        # Determine block pattern based on block_type
        block_pattern = None
        if block_type in ['resource', 'data']:
            if '.' in block_name:
                _, type_name, instance_name = block_name.split('.', 2)
                block_pattern = rf'{block_type}\s+"{re.escape(type_name)}"\s+"{re.escape(instance_name)}"\s*{{'
            else:
                block_pattern = rf'{block_type}\s+"[^"]+"\s+"{re.escape(block_name)}"\s*{{'
        elif block_type in ['provider', 'variable', 'output', 'module']:
            block_pattern = rf'{block_type}\s+"{re.escape(block_name)}"\s*{{'
        elif block_type in ['terraform', 'locals']:
            block_pattern = rf'{block_type}\s*{{'
        elif block_type == 'fallback' or block_type == 'import':
            block_pattern = re.escape(content_str.splitlines()[0].strip()) if content_str.strip() else None
        
        start_line = 0
        end_line = 0
        in_block = False
        brace_count = 0
        
        # Search for block start, ignoring comment lines
        for i, line in enumerate(lines, 1):
            line_content = line.strip()
            if line_content.startswith('#'):
                continue
            if block_pattern and not in_block and re.search(block_pattern, line_content, re.IGNORECASE):
                start_line = i
                in_block = True
                brace_count += line_content.count('{') - line_content.count('}')
            elif in_block:
                brace_count += line_content.count('{') - line_content.count('}')
                if brace_count <= 0:
                    end_line = i
                    break
        
        # Fallback: Match content directly if pattern fails
        if start_line == 0 and content_str.strip():
            chunk_lines = [l.strip() for l in content_str.splitlines() if l.strip() and not l.strip().startswith('#')]
            if chunk_lines:
                first_chunk_line = chunk_lines[0]
                for i, line in enumerate(lines, 1):
                    if line.strip().startswith('#'):
                        continue
                    if first_chunk_line in line.strip() or line.strip().startswith(first_chunk_line.split()[0]):
                        start_line = i
                        end_line = start_line + len(chunk_lines) - 1
                        break
        
        if start_line == 0 or end_line == 0:
            print(f"Warning: Could not determine lines for {block_type} {block_name} in {file_path}. Using fallback: 1-{len(lines)}")
            start_line = 1
            end_line = len(lines)
        
        print(f"Calculated lines for {block_type} {block_name}: {start_line}-{end_line}")
        return start_line, end_line
    except Exception as e:
        print(f"Error calculating lines for {file_path}: {e}")
        return 0, 0

def format_content(chunk_content, block_type, block_name):
    """Format chunk content as HCL text or JSON for policies"""
    if isinstance(chunk_content, str):
        return chunk_content.strip()
    
    def to_hcl(d, indent=0, is_label=False):
        lines = []
        if isinstance(d, str):
            lines.append('  ' * indent + f'"{d}"')
            return lines
        for key, value in d.items():
            if is_label:
                lines.append('  ' * indent + f'"{key}" {{' if isinstance(value, dict) else f'"{key}" = {json.dumps(value)}')
                if isinstance(value, dict):
                    lines.extend(to_hcl(value, indent + 1))
                    lines.append('  ' * indent + '}')
            else:
                if isinstance(value, dict):
                    lines.append('  ' * indent + f'{key} {{')
                    lines.extend(to_hcl(value, indent + 1))
                    lines.append('  ' * indent + '}')
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            lines.append('  ' * indent + f'{key} {{')
                            lines.extend(to_hcl(item, indent + 1))
                            lines.append('  ' * indent + '}')
                        else:
                            lines.append('  ' * indent + f'{key} = {json.dumps(item)}')
                else:
                    lines.append('  ' * indent + f'{key} = {json.dumps(value)}')
        return lines
    
    # Handle different block types
    if block_type in ['resource', 'data']:
        if '.' in block_name:
            _, type_name, instance_name = block_name.split('.', 2)
            if block_type in chunk_content and type_name in chunk_content[block_type] and instance_name in chunk_content[block_type][type_name]:
                lines = [f'{block_type} "{type_name}" "{instance_name}" {{']
                lines.extend(to_hcl(chunk_content[block_type][type_name][instance_name], 1))
                lines.append('}')
                return '\n'.join(lines)
    elif block_type in ['provider', 'variable', 'output', 'module']:
        if block_type in chunk_content and block_name in chunk_content[block_type]:
            lines = [f'{block_type} "{block_name}" {{']
            lines.extend(to_hcl(chunk_content[block_type][block_name], 1))
            lines.append('}')
            return '\n'.join(lines)
    elif block_type in ['terraform', 'locals']:
        if block_type in chunk_content:
            lines = [f'{block_type} {{']
            lines.extend(to_hcl(chunk_content[block_type], 1, is_label=(block_type == 'locals')))
            lines.append('}')
            return '\n'.join(lines)
    return json.dumps(chunk_content, indent=2)

def generate_chunks(config, file_path):
    """Phase 5: Chunk generation"""
    chunks = []
    if not config:
        return chunks
    
    for block_type in ['terraform', 'provider', 'resource', 'module', 'data', 'variable', 'output', 'locals']:
        blocks = config.get(block_type, [])
        if not isinstance(blocks, list):
            continue
        for block in blocks:
            if not isinstance(block, dict):
                continue
            if block_type in ['terraform', 'locals']:
                full_block_name = block_type
                chunk_content = {block_type: block}
                chunks.append((chunk_content, block_type, full_block_name))
            else:
                for label1, content in block.items():
                    if block_type in ['resource', 'data']:
                        if isinstance(content, dict):
                            for label2, attrs in content.items():
                                full_block_name = f"{block_type}.{label1}.{label2}"
                                chunk_content = {block_type: {label1: {label2: attrs}}}
                                chunks.append((chunk_content, block_type, full_block_name))
                    else:
                        full_block_name = label1
                        chunk_content = {block_type: {label1: content}}
                        chunks.append((chunk_content, block_type, full_block_name))
    return chunks

def fallback_chunking(file_path, target_size=400, overlap=50):
    """Phase 6: Fallback - regex + line-based"""
    chunks = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return chunks
    
    block_pattern = re.compile(r'(resource|data|module|provider|terraform|variable|output|locals)\s+(")?([^"\s}]+)?(")?\s*(")?([^"\s}]+)?(")?\s*{((?:[^{}]+|{[^{}]*})*)}', re.DOTALL)
    matches = block_pattern.finditer(content)
    
    for match in matches:
        chunk = match.group(0)
        block_type = match.group(1)
        label1 = match.group(3).strip('"') if match.group(3) else ''
        label2 = match.group(6).strip('"') if match.group(6) else ''
        block_name = f"{block_type}.{label1}.{label2}" if label2 and block_type in ['resource', 'data'] else label1 if label1 else block_type
        chunks.append((chunk, block_type, block_name))
    
    if not matches:
        lines = content.splitlines()
        current_chunk = []
        current_tokens = 0
        for line in lines:
            tokens = len(line.split())
            if current_tokens + tokens > target_size:
                chunks.append(('\n'.join(current_chunk[-overlap:]), 'fallback', 'chunk'))
                current_chunk = current_chunk[-overlap:] + [line]
                current_tokens = sum(len(l.split()) for l in current_chunk)
            else:
                current_chunk.append(line)
                current_tokens += tokens
        if current_chunk:
            chunks.append(('\n'.join(current_chunk), 'fallback', 'chunk'))
    
    return chunks

def attach_metadata(chunk, file_path, start_line, end_line, block_type, block_name, module_path, region='unknown'):
    """Phase 7: Attach metadata"""
    metadata = {
        'repo': 'local',
        'commit': 'none',
        'file': file_path,
        'lines': f"{start_line}-{end_line}",
        'resource_address': block_name,
        'resource_type': block_type,
        'module': module_path or 'none',
        'account': 'unknown',
        'region': region,
        'content': format_content(chunk, block_type, block_name)
    }
    return metadata

def special_handling(config, chunks, file_path):
    """C. Special handling"""
    if not config:
        return chunks
    region = get_region(config)
    module_path = get_module_path(file_path)
    processed_blocks = set()
    for block_type in ['variable', 'locals', 'module']:
        blocks = config.get(block_type, [])
        if not isinstance(blocks, list):
            continue
        for block in blocks:
            if not isinstance(block, dict):
                continue
            for label, content in block.items():
                chunk_key = (block_type, label)
                if chunk_key not in processed_blocks:
                    block_name = block_type if block_type == 'locals' else label
                    # For locals, wrap content in a dictionary to avoid string issues
                    chunk_content = {block_type: {label: content}} if block_type == 'locals' else {block_type: {label: content}}
                    start_line, end_line = calculate_lines(file_path, chunk_content, block_type, block_name)
                    meta_chunk = attach_metadata(chunk_content, file_path, start_line, end_line, block_type, block_name, module_path, region)
                    chunks.append(meta_chunk)
                    processed_blocks.add(chunk_key)
    return chunks

def get_region(config):
    """Extract region from provider block"""
    for provider in config.get('provider', []):
        for _, provider_content in provider.items():
            return provider_content.get('region', 'unknown')
    return 'unknown'

def get_module_path(file_path):
    """Determine module path if file is in a modules/ directory"""
    if 'modules/' in file_path:
        parts = file_path.split('modules/')
        if len(parts) > 1:
            module_dir = parts[1].split('/')[0]
            return f"modules/{module_dir}"
    return 'none'