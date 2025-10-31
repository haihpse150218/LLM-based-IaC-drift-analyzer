import os
import json
import uuid
from utils import *
from datetime import datetime, timezone

MAX_BYTES_PER_FILE = 20_000  # ⚠️ Giới hạn an toàn cho Bedrock (≈ 25 KB)

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
                config = canonicalize(config)
                config = resolve_variables(config, tfvars_path)
                file_chunks = generate_chunks(config, file_path)
            else:
                print(f"Falling back to regex for {file_path}")
                file_chunks = fallback_chunking(file_path)

            for chunk_content, block_type, block_name in file_chunks:
                start_line, end_line = calculate_lines(
                    file_path, chunk_content, block_type,
                    block_name.split('.')[-1] if '.' in block_name else block_name
                )

                if isinstance(chunk_content, str):
                    chunk_content = {'fallback': {'content': chunk_content}}
                    block_type = 'fallback'
                    block_name = 'import' if 'terraform import' in chunk_content else block_name

                meta_chunk = attach_metadata(
                    chunk_content, file_path, start_line, end_line,
                    block_type, block_name, module_path, region
                )
                chunks.append(meta_chunk)

            if config:
                chunks.extend(special_handling(config, [], file_path))

    return chunks

if __name__ == '__main__':
    directory = 'RESSOURCE'
    tfvars_path = None
    base_name = "terraform-on-aws-ec2" # giả sử https://github.com/haihpse150218/terraform-on-aws-ec2.git sẽ lấy base_name = terraform-on-aws-ec2 -> OUTPUT có dạng OUTPUT/terraform-on-aws-ec2/file.jsonl
    output_dir = os.path.join('Output', base_name)

    # 🔧 Tạo thư mục Output/<base_name> nếu chưa có
    os.makedirs(output_dir, exist_ok=True)

    # Auto detect tfvars
    for root, _, files in os.walk(directory):
        if 'vars.tfvars' in files:
            tfvars_path = os.path.join(root, 'vars.tfvars')
            break

    try:
        results = []
        chunks = process_directory(directory, tfvars_path)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        file_index = 1
        current_size = 0
        buffer = []

        def write_split_file(buffer_data, idx):
            """Write one JSONL file safely into Output/<base_name>/ folder."""
            output_file = os.path.join(output_dir, f"{base_name}_{idx}.jsonl")
            with open(output_file, "w", encoding="utf-8") as f:
                f.writelines(buffer_data)
            size_kb = sum(len(x.encode("utf-8")) for x in buffer_data) / 1024
            print(f"✅ Saved {output_file} ({len(buffer_data)} chunks, {size_kb:.1f} KB)")

        for chunk in chunks:
            chunk['type'] = "iac_configuration"
            chunk['id'] = str(uuid.uuid1())
            chunk['update_at'] = timestamp
            chunk['owner'] = 'haihpse150218'
            chunk['repo'] = 'https://github.com/haihpse150218/terraform-on-aws-ec2.git'
            chunk['metadata'] = {
                "repo": "https://github.com/haihpse150218/terraform-on-aws-ec2.git",
                "commit": "none",
                "owner": "none",
                "region": "us-east-1",
                "account": "none",
                "type": "iac_configuration"
            }

            line = json.dumps(chunk, ensure_ascii=False) + "\n"
            line_bytes = len(line.encode("utf-8"))

            if current_size + line_bytes > MAX_BYTES_PER_FILE:
                write_split_file(buffer, file_index)
                file_index += 1
                buffer = []
                current_size = 0

            buffer.append(line)
            current_size += line_bytes
            results.append(chunk)

        if buffer:
            write_split_file(buffer, file_index)

        print('-' * 60)
        print(f"🎯 Generated {file_index} JSONL files in folder '{output_dir}', total {len(results)} chunks.")

    except Exception as e:
        print(f"❌ Error processing directory: {e}")
