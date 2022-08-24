
"""
Write a dag for bash operator. 
"""

#
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

"""Example DAG demonstrating the usage of the BashOperator."""

import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='GCS_BQ_bash_operator',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2022, 8, 18),
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['gcs', 'bq'],
) as dag:
    run_this_1 = BashOperator(
        task_id='run_first',
        bash_command='bq mk mydataset',
    )
    run_this_2 = BashOperator(
        task_id='run_after',
        bash_command='bq load --autodetect --source_format=CSV mydataset.mytable gs://bucket_name/table_name.xls',
    )

    run_this_3 = BashOperator(
        task_id='run_last_for_cleanup',
        bash_command='bq rm airflowTPCH',
    )

    run_this_1 >> run_this_2 >> run_this_3

if __name__ == "__main__":
    dag.cli()
