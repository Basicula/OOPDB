rmdir /s /q dist
set version=%1
python -m build
pip install --force-reinstall dist/oopdb-%version%-py3-none-any.whl