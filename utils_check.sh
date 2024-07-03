#!/bin/bash

# Update the files in the `utils` directory of a Python virtual environment.
#
# This script checks if the files in the `/src/utils` directory are 
# different from the ones in the `site-packages` directory of the virtual environment. 
# If they are different, it copies the files from `/src/utils` to the
# `site-packages` directory.
#
# Attributes:
#     UTILS_DIR (str): The path to the `utils` directory.
#     VENV_DIR (str): The path to the virtual environment directory.
#     SITE_PACKAGES_DIR (str): The path to the `site-packages` directory 
#     of the virtual environment.

UTILS_DIR="src/utils"
VENV_DIR="src/lambda_functions/utils_layer/python"
SITE_PACKAGES_DIR="$VENV_DIR/lib/python3.9/site-packages"

for file in "$UTILS_DIR"/*.py; do
    filename=$(basename -- "$file")
    if ! cmp -s "$file" "$SITE_PACKAGES_DIR/src/utils/$filename"; then
        echo "Updating $filename"
        cp "$file" "$SITE_PACKAGES_DIR/src/utils/$filename"
    fi
done
