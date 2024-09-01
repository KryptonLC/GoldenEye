import os
from typing import List

# List of file extensions to exclude
EXCLUDE_EXTENSIONS = ['.xlsx', '.xls', '.csv', '.md', '.pyc', '.docx', '.log']  # Add any other file types to exclude here

# Dynamically add the current script file to the exclude list
CURRENT_SCRIPT = os.path.basename(__file__)

# List of specific files to exclude (provide only the filename)
EXCLUDE_FILES = [
    CURRENT_SCRIPT,  # Automatically exclude the script itself
    'excluded_file.py',  # Example of a specific file to exclude
    'exclude_this.py'
]

# List of folders to exclude
EXCLUDE_FOLDERS = [
    '.git',  # Example of a folder to exclude
    '__pycache__',  # Another common folder to exclude
]

def should_exclude_folder(folder_name: str, exclude_folders: List[str]) -> bool:
    """
    Determines if the folder should be excluded based on its name.
    """
    return folder_name in exclude_folders

def should_exclude_file(filename: str, exclude_extensions: List[str], exclude_files: List[str]) -> bool:
    """
    Determines if the file should be excluded based on its extension or specific filename.
    """
    _, ext = os.path.splitext(filename)
    return ext in exclude_extensions or filename in exclude_files

def generate_folder_structure(root_dir: str, exclude_extensions: List[str], exclude_files: List[str], exclude_folders: List[str]) -> str:
    """
    Generates a folder structure diagram as a string with filenames.
    """
    folder_structure = ""
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Skip folders that should be excluded
        dirnames[:] = [d for d in dirnames if not should_exclude_folder(d, exclude_folders)]
        
        level = dirpath.replace(root_dir, '').count(os.sep)
        indent = ' ' * 4 * level
        folder_structure += f"{indent}{os.path.basename(dirpath)}/\n"
        sub_indent = ' ' * 4 * (level + 1)
        for filename in filenames:
            if not should_exclude_file(filename, exclude_extensions, exclude_files):
                folder_structure += f"{sub_indent}{filename}\n"
    return folder_structure

def write_code_to_md(root_dir: str, output_file: str, 
                     exclude_extensions: List[str], exclude_files: List[str], exclude_folders: List[str]) -> None:
    """
    Recursively traverses the directory structure and writes code to a markdown file.
    """
    with open(output_file, 'w', encoding='utf-8') as md_file:
        # Write the folder structure at the beginning of the file
        md_file.write("# Project Folder Structure\n\n")
        folder_structure = generate_folder_structure(root_dir, exclude_extensions, exclude_files, exclude_folders)
        md_file.write(f"```\n{folder_structure}\n```\n\n")
        
        # Write code from each file into the markdown
        md_file.write("# Project Code\n\n")
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Skip folders that should be excluded
            dirnames[:] = [d for d in dirnames if not should_exclude_folder(d, exclude_folders)]

            for filename in filenames:
                if not should_exclude_file(filename, exclude_extensions, exclude_files):
                    md_file.write(f"## {os.path.basename(dirpath)}\n\n")
                    md_file.write(f"### {filename}\n\n")
                    md_file.write("```python\n")
                    file_path = os.path.join(dirpath, filename)
                    
                    try:
                        with open(file_path, 'r', encoding='utf-8') as code_file:
                            md_file.write(code_file.read())
                    except UnicodeDecodeError:
                        print(f"Skipping file due to encoding issues: {file_path}")
                    
                    md_file.write("\n```\n\n")

if __name__ == "__main__":
    # Specify the root directory and output file
    root_directory = '.'  # Start from the directory where the script is placed
    output_markdown_file = 'project_code.md'

    # Generate the markdown file
    write_code_to_md(root_directory, output_markdown_file, EXCLUDE_EXTENSIONS, EXCLUDE_FILES, EXCLUDE_FOLDERS)