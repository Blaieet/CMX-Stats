
import os
import shutil
import re

SOURCE_DIR = "images"
DEST_DIR = "assets/players"

if not os.path.exists(DEST_DIR):
    os.makedirs(DEST_DIR)

def slugify(text):
    text = str(text).lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_-]+', '-', text)
    return text

print("Scanning images directory...")
count = 0

for root, dirs, files in os.walk(SOURCE_DIR):
    for file in files:
        if file.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):
            # Check if parent folder matches a player name structure (e.g., uppercase)
            parent_folder = os.path.basename(root)
            
            # Skip the root image folder itself if it contains loose files (like crest)
            if root == SOURCE_DIR:
                continue
                
            # Rename logic: Use parent folder name as player name
            player_slug = slugify(parent_folder)
            ext = os.path.splitext(file)[1]
            new_filename = f"{player_slug}{ext}"
            
            src_path = os.path.join(root, file)
            dest_path = os.path.join(DEST_DIR, new_filename)
            
            print(f"Copying {src_path} -> {dest_path}")
            shutil.copy2(src_path, dest_path)
            count += 1

print(f"Processed {count} images.")
