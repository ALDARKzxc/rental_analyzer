"""
PyInstaller hook для пакета playwright.
Собирает все нужные бинарники playwright (node.exe, драйверы).
"""
from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs

datas   = collect_data_files('playwright')
binaries = collect_dynamic_libs('playwright')
