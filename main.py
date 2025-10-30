import os
import json
import uuid
from utils import *
from datetime import datetime, timezone

def process_directory(directory, tfvars_path=None):
    chunks = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_type = detect_file_type(file_path)
            if file_type not in ['terraform', 'tfvars']:
                print(f"Skipping non-Terraform file: {file_path}")
                continue
            
            print(f"Processing file: {file_path}")
            config = parse_ast(file_path)
            region = get_region(config) if config else 'unknown'
            module_path = get_module_path(file_path)
            
            if config:
                print(f"Parsed config for {file_path}: {json.dumps(config, indent=2)}")
                config = canonicalize(config)
                config = resolve_variables(config, tfvars_path)
                file_chunks = generate_chunks(config, file_path)
            else:
                print(f"Falling back to regex for {file_path}")
                file_chunks = fallback_chunking(file_path)
            
            # Process chunks from generate_chunks or fallback_chunking
            for chunk_content, block_type, block_name in file_chunks:
                start_line, end_line = calculate_lines(file_path, chunk_content, block_type, block_name.split('.')[-1] if '.' in block_name else block_name)
                # Handle string content from fallback_chunking
                if isinstance(chunk_content, str):
                    chunk_content = {'fallback': {'content': chunk_content}}
                    block_type = 'fallback'
                    block_name = 'import' if 'terraform import' in chunk_content else block_name
                meta_chunk = attach_metadata(chunk_content, file_path, start_line, end_line, block_type, block_name, module_path, region)
                chunks.append(meta_chunk)
            
            # Process chunks from special_handling (now returns dicts)
            if config:
                chunks.extend(special_handling(config, [], file_path))
    
    return chunks

def format_chunk(chunk):
    """Format chunk as requested"""
    metadata = chunk.copy()  # Use copy to avoid modifying the original dict
    content = metadata.pop('content')
    lines = '\n'.join(f"{key}: {value}" for key, value in metadata.items())
    return f"=== RESOURCE CHUNK ===\n{lines}\n---CONTENT---\n{content}"

if __name__ == '__main__':
    directory = 'RESSOURCE'
    tfvars_path = None
    for root, _, files in os.walk(directory):
        if 'vars.tfvars' in files:
            tfvars_path = os.path.join(root, 'vars.tfvars')
            break
    
    try:
        results = []
        chunks = process_directory(directory, tfvars_path)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for chunk in chunks:
            formatted = format_chunk(chunk)
            chunk['type'] = "iac_configuration"
            chunk['id'] = str(uuid.uuid1())
            chunk['timestamp'] = timestamp
            chunk['owner'] = 'unknow'
            print(formatted)
            results.append(chunk)
            print('-' * 50)
        print(f"Processed {len(chunks)} chunks")
        with open("output.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Error processing directory: {e}")