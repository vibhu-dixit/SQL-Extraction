import cv2
import pytesseract
import numpy as np
from sklearn.cluster import DBSCAN
import re
import json

# 1. OCR Wrapper
def extract_text_boxes(img_path):
    img = cv2.imread(img_path)
    data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
    boxes = []
    for i in range(len(data['text'])):
        conf = int(data['conf'][i])
        if conf > 0:  # Filter out -1
            x, y, w, h = data['left'][i], data['top'][i], data['width'][i], data['height'][i]
            text = data['text'][i]
            boxes.append({'bbox': (x, y, w, h), 'text': text, 'conf': conf})
    return boxes

# 2. Bounding-Box Cleaning
def clean_boxes(boxes, conf_threshold=50):
    return [
        b for b in boxes
        if b['conf'] >= conf_threshold and len(b['text'].strip()) > 1 and b['text'].isalnum()
    ]

# 3. DBSCAN Clustering
def cluster_boxes(boxes):
    X = np.array([[b['bbox'][0], b['bbox'][1]] for b in boxes])
    clustering = DBSCAN(eps=50, min_samples=2).fit(X)
    for box, label in zip(boxes, clustering.labels_):
        box['cluster'] = label
    return boxes

# 4. Key/Value Tagging (simple heuristic)
def tag_keys_values(boxes):
    key_value_pairs = []
    clusters = {}
    for b in boxes:
        if b['cluster'] == -1:
            continue
        clusters.setdefault(b['cluster'], []).append(b)
    
    for cluster_id, group in clusters.items():
        group = sorted(group, key=lambda x: (x['bbox'][1], x['bbox'][0]))
        for i in range(len(group) - 1):
            key = group[i]['text']
            value = group[i+1]['text']
            if key.isupper():  # Heuristic for keys
                key_value_pairs.append((key, value))
    return key_value_pairs

# 5. Schema Assembly
def assemble_schema(kv_pairs):
    output = {"name": "Form1", "columns": []}
    for key, value in kv_pairs:
        if re.match(r"\d{4}-\d{2}-\d{2}", value):
            col_type = "DATE"
        elif value.isdigit():
            col_type = "INT"
        else:
            col_type = "TEXT"
        output["columns"].append({"col": key, "type": col_type})
    return output

# Main pipeline
def process_form(image_path):
    boxes = extract_text_boxes(image_path)
    boxes = clean_boxes(boxes)
    boxes = cluster_boxes(boxes)
    kv_pairs = tag_keys_values(boxes)
    schema = assemble_schema(kv_pairs)
    return schema

if __name__ == "__main__":
    image_path = "cricket.png"  # Replace with your image path
    result = process_form(image_path)
    print(json.dumps(result, indent=2))
