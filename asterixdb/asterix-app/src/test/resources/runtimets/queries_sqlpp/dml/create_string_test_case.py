
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
import os
import shutil

STRING_AS_PRIMARY_KEY = "-string-as-primary-key"

folder_name_old = sys.argv[1][:-22]
folder_name_new = folder_name_old + STRING_AS_PRIMARY_KEY

os.mkdir(folder_name_new)

print folder_name_old
print folder_name_new

for file_name_old in os.listdir(folder_name_old):
    splits = file_name_old.split(".")
    file_name_new = splits[0] + STRING_AS_PRIMARY_KEY
    for name in splits[1:]:
        file_name_new += "." + name
    shutil.copy2(folder_name_old + "/" + file_name_old, folder_name_new + "/" + file_name_new)

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
