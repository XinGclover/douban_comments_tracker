#!/bin/bash

BUCKET="zhaoxuelu-dashboard-bucket"
LOCAL_DIR="./export/shujuanyimeng_view_export"
DEST_DIR="shujuanyimeng" 

for file_path in "$LOCAL_DIR"/*; do
    file_name=$(basename "$file_path")
    object_key="$DEST_DIR/$file_name"
    echo "Upload $file_path to bos://$BUCKET/$object_key ..."
    bce-cli bos put-object --bucket "$BUCKET" --key "$object_key" --file "$file_path"
    if [ $? -eq 0 ]; then
        echo "✅ Upload Successfully $file_name"
    else
        echo "❌ Upload Failed $file_name"
    fi
done
