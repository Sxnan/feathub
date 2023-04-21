#  Copyright 2022 The FeatHub Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import glob
import os

from pyflink.table import StreamTableEnvironment

from feathub.common.exceptions import FeathubException
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env


def load_format(t_env: StreamTableEnvironment, data_format: str) -> None:
    if data_format == "avro":
        return _load_avro_format_jar(t_env)

    return


def _load_avro_format_jar(t_env: StreamTableEnvironment) -> None:
    avro_jar_path = _get_jar_path("flink-sql-avro-*.jar")
    add_jar_to_t_env(t_env, avro_jar_path)


def _get_jar_path(jar_files_pattern: str) -> str:
    lib_dir = find_jar_lib()
    jars = glob.glob(os.path.join(lib_dir, jar_files_pattern))
    if len(jars) < 1:
        raise FeathubException(
            f"Can not find the jar at {lib_dir} with pattern {jar_files_pattern}."
        )
    return jars[0]
