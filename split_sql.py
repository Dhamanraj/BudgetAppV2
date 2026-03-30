import os
import re
import argparse

def split_mysql_dump(input_file, base_output_dir, strip_definer=True):
    # 1. Define target directories
    dirs = {
        'TABLE': os.path.join(base_output_dir, 'tables'),
        'PROCEDURE': os.path.join(base_output_dir, 'procedures'),
        'VIEW': os.path.join(base_output_dir, 'views'),
        'TRIGGER': os.path.join(base_output_dir, 'triggers')
    }

    # Create directories if they don't exist
    for path in dirs.values():
        os.makedirs(path, exist_ok=True)

    # 2. Read the dump file
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 3. Use Regex to find CREATE statements
    # Identify the start of every object. This is more reliable than finding the end.
    start_pattern = re.compile(
        r'CREATE\s+(?:DEFINER=`[^`]+`@`[^`]+`\s+)?(?P<type>TABLE|PROCEDURE|VIEW|TRIGGER)\s+(?:IF NOT EXISTS\s+)?`(?P<name>[^`]+)`',
        re.IGNORECASE
    )
    
    matches = list(start_pattern.finditer(content))
    count = {k: 0 for k in dirs.keys()}

    for i, match in enumerate(matches):
        obj_type = match.group('type').upper()
        obj_name = match.group('name')
        start_pos = match.start()
        
        # The chunk ends where the next object starts, or at the end of the file
        end_pos = matches[i+1].start() if i + 1 < len(matches) else len(content)
        chunk = content[start_pos:end_pos].strip()

        # Refine the object body based on its type
        if obj_type in ('TABLE', 'VIEW'):
            # Tables/Views end at the first semicolon
            body_match = re.search(r'.*?;', chunk, re.DOTALL)
            obj_body = body_match.group(0) if body_match else chunk
        else:
            # Procedures/Triggers: find the LAST 'END' followed by a delimiter
            # We use a greedy match (.*) to bypass internal IF/CASE ENDs.
            # Added \s+ before END and improved delimiter detection to prevent
            # matching 'END' inside words like 'p_Gender' or 'p_EndDate'
            block_end_pattern = re.compile(r'(?s).*\bEND\s*(?:;;|//|;)')
            body_match = block_end_pattern.search(chunk)
            if body_match:
                obj_body = body_match.group(0)
            else:
                # Fallback: take the chunk but remove any trailing DELIMITER commands
                obj_body = re.split(r'(?i)DELIMITER', chunk)[0].strip()

        # 4. Clean up machine-specific noise (like AUTO_INCREMENT values)
        obj_body = re.sub(r'AUTO_INCREMENT=\d+\s+', '', obj_body)

        if strip_definer:
            obj_body = re.sub(r'DEFINER=`[^`]+`@`[^`]+`', '', obj_body)
            
        # Remove any lingering delimiter definitions from the end
        obj_body = re.sub(r'(?i)DELIMITER\s*.*', '', obj_body).strip()

        if obj_type in dirs:
            file_path = os.path.join(dirs[obj_type], f"{obj_name}.sql")
            with open(file_path, 'w', encoding='utf-8') as out_f:
                # Only add a semicolon if the body doesn't already end with one or a delimiter
                if not any(obj_body.endswith(d) for d in (';', '//', ';;')):
                    out_f.write(obj_body + ";\n")
                else:
                    out_f.write(obj_body + "\n")
            count[obj_type] += 1

    print(f"Successfully split objects into '{base_output_dir}':")
    for dtype, c in count.items():
        print(f"  - {dtype}s: {c}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split a MySQL dump into individual files.")
    parser.add_argument("--input", default="/Users/dhamankumarkakke/Documents/Project/GitHub/full_schema.sql", help="Path to full SQL dump")
    parser.add_argument("--output", default="/Users/dhamankumarkakke/Documents/Project/GitHub/BudgetAppV2", help="Base output directory")
    
    args = parser.parse_args()
    split_mysql_dump(args.input, args.output)