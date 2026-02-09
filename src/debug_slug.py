import re

def slugify(text):
    text = str(text).lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_-]+', '-', text)
    return text

name = "VIDAL ESPUNY, ADRIÀ"
slug = slugify(name)
print(f"Name: {name}")
print(f"Slug: {slug}")
