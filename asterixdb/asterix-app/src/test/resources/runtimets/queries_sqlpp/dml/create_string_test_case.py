#!/usr/bin/python

import sys
import os
import shutil

STRING_AS_PRIMARY_KEY = "-string-as-primary-key"

folder_name_old = sys.argv[1]
folder_name_new = folder_name_old + STRING_AS_PRIMARY_KEY

# os.mkdir(folder_name_new)

print folder_name_old
print folder_name_new

for file_name_old in os.listdir(folder_name_old):
    splits = file_name_old.split(".")
    file_name_new = splits[0] + STRING_AS_PRIMARY_KEY
    for name in splits[1:]:
        file_name_new += "." + name
    # shutil.copy2(folder_name_old + "/" + file_name_old, folder_name_new + "/" + file_name_new)

old_path = os.getcwd()
new_path = old_path.replace("queries_sqlpp", "results", 1)

os.chdir(new_path)
os.mkdir(folder_name_new)

for file_name_old in os.listdir(folder_name_old):
    splits = file_name_old.split(".")
    file_name_new = splits[0] + STRING_AS_PRIMARY_KEY
    for name in splits[1:]:
        file_name_new += "." + name
    shutil.copy2(folder_name_old + "/" + file_name_old, folder_name_new + "/" + file_name_new)
