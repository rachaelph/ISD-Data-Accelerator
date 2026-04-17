"""Script to merge unit test files into 3 balanced files."""
import os

# Get paths relative to this script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)
source_dir = os.path.join(repo_root, 'integration_tests', 'notebooks')
target_dir = script_dir  # Output to same folder as this script

# Get all Python files with line counts
files = []
for f in os.listdir(source_dir):
    if f.endswith('.py'):
        path = os.path.join(source_dir, f)
        with open(path, 'r', encoding='utf-8') as file:
            lines = len(file.readlines())
        files.append((f, lines, path))

print(f"Found {len(files)} Python files totaling {sum(f[1] for f in files)} lines")

# Sort by line count descending for better distribution
files.sort(key=lambda x: -x[1])

# Distribute into 3 buckets using greedy algorithm
buckets = [[], [], []]
bucket_sizes = [0, 0, 0]

for f, lines, path in files:
    min_idx = bucket_sizes.index(min(bucket_sizes))
    buckets[min_idx].append((f, lines, path))
    bucket_sizes[min_idx] += lines

# Create merged files
for i, bucket in enumerate(buckets):
    output_path = os.path.join(target_dir, f'merged_tests_{i+1}.py')
    with open(output_path, 'w', encoding='utf-8') as out:
        # Write header
        out.write('"""\n')
        out.write(f'Merged Test File {i+1}\n')
        out.write('=' * 20 + '\n')
        out.write('This file contains merged unit tests from the following source files:\n')
        for f_item in bucket:
            out.write(f'- {f_item[0]} ({f_item[1]} lines)\n')
        out.write(f'\nTotal lines: ~{bucket_sizes[i]}\n')
        out.write('"""\n\n')
        
        # Write each source file
        for f_name, lines, path in bucket:
            with open(path, 'r', encoding='utf-8') as src:
                content = src.read()
            out.write('# ' + '=' * 78 + '\n')
            out.write(f'# SOURCE: {f_name} ({lines} lines)\n')
            out.write('# ' + '=' * 78 + '\n\n')
            out.write(content)
            out.write('\n\n')
    
    print(f'\nCreated {output_path}')
    print(f'  Files: {[f[0] for f in bucket]}')
    print(f'  Total lines: {bucket_sizes[i]}')

print('\nDone!')
