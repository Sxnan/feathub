# Copyright 2022 The Feathub Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import List, Any

from feathub.common.utils import to_java_date_format


def evaluate_function(func_name: str, args: List[Any]) -> str:
    if func_name.upper() == "UNIX_TIMESTAMP" and len(args) > 1:
        args[1] = to_java_date_format(args[1])
    return f"{func_name}({', '.join(args)})"