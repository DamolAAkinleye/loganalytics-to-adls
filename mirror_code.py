import os
import sys
from pathlib import Path

def create_mirror_with_md(source_dir, target_dir):
    """
    Creates a mirror directory structure with Markdown versions of Python files.
    
    Args:
        source_dir: The source directory containing Python files
        target_dir: The target directory where Markdown files will be created
    """
    source_path = Path(source_dir)
    target_path = Path(target_dir)
    
    # Create the target directory if it doesn't exist
    target_path.mkdir(parents=True, exist_ok=True)
    
    # Walk through the source directory
    for root, dirs, files in os.walk(source_path):
        # Create relative path to maintain directory structure
        rel_path = Path(root).relative_to(source_path)
        current_target_dir = target_path / rel_path
        
        # Create directories in the target
        current_target_dir.mkdir(parents=True, exist_ok=True)
        
        # Process files
        for file in files:
            if file.endswith('.py'):
                source_file = Path(root) / file
                # Create markdown file with the same name but .md extension
                target_file = current_target_dir / f"{file}.md"
                
                # Read Python file content
                with open(source_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Write content to Markdown file with Python syntax highlighting
                with open(target_file, 'w', encoding='utf-8') as f:
                    f.write(f"# {file}\n\n")
                    f.write("```python\n")
                    f.write(content)
                    f.write("\n```\n")
                
                print(f"Converted {source_file} to {target_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python mirror_code.py <source_directory> <target_directory>")
        sys.exit(1)
    
    source_dir = sys.argv[1]
    target_dir = sys.argv[2]
    
    create_mirror_with_md(source_dir, target_dir)
    print(f"Mirror directory with Markdown files created at {target_dir}") 