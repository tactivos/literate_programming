import logging
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from src.common.lib import dag as airflow_dag
from src.common.lib.constants import DBT_PROJECT_DIR, DBT_PROFILES_DIR
from src.common.lib.dbt_manifest_parser import DbtManifestParser
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from src.common.task.airflow_tasks import wait_for

def _refresh_deps():
    deps_command = f'dbt deps --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}'.split(' ')
    logging.info(f"Running DBT Deps: {deps_command}")
    return subprocess.run(deps_command, check=True)


def _refresh_manifest():
    compile_command = f'dbt --no-partial-parse compile --profiles-dir {DBT_PROFILES_DIR} --project-dir' \
                      f' {DBT_PROJECT_DIR}'.split(' ')
    logging.info(f"Refreshing DBT Manifest: {compile_command}")
    return subprocess.run(compile_command, capture_output=True, check=True)


def _execute():
    print('I am here 1')
    subproccess_deps = _refresh_deps()
    if subproccess_deps.returncode == 0:
        logging.info('DBT packages successfully refreshed')

    subproccess_compile = _refresh_manifest()
    if subproccess_compile.returncode == 0:
        logging.info('DBT manifest file successfully refreshed')

    print('I am here 2')
    dbt_manifest_parser = DbtManifestParser()
    print('~' * 20)
    for dag_id in dbt_manifest_parser.get_all_dag_names():

        print('I am here 3: ' + dag_id)
        dag: DAG = airflow_dag.create(
            execution_timeout=timedelta(seconds=60),
            description="DAG generated from DBT manifest file",
            dag_owner=airflow_dag.DAGOwner.data_platform,
            dag_id=dag_id,
            start_date=datetime(year=2022, month=7, day=29,
                                hour=0, minute=0, second=0),
            schedule_interval="@daily"
        )
        print("Created DAG: " + dag_id)
        print('~' * 20)
        with dag:
            g = dbt_manifest_parser.get_dag(dag_id)
            for n, d in g.nodes(data=True):
                print(n, d)

            task_nodes = {}

            for node, data in g.nodes(data=True):
                if data['type'] in ['model', 'snapshot', 'test']:
                    task_nodes[node] = BashOperator(
                        task_id=node,
                        bash_command=data['command'],
                        dag=dag,
                    )
            for edge in g.edges():
                if g.nodes[edge[1]]['type'] == 'wait_for':
                    wait_for_node = edge[1]
                    if wait_for_node not in task_nodes:
                        external_dag_id = g.nodes[edge[0]]['dag_name']
                        external_task_id = edge[0]
                        task_nodes[wait_for_node] = wait_for(
                            dag,
                            task_id=wait_for_node,
                            external_dag_id=external_dag_id,
                            external_task_id=external_task_id
                        )

            for edge in g.edges():
                task_nodes[edge[0]].set_downstream(task_nodes[edge[1]])

            start_dummy = DummyOperator(task_id='start')
            end_dummy = DummyOperator(task_id='end')
            start_dummy.set_downstream([task_nodes[n] for n in g.nodes() if g.in_degree(n) == 0])
            end_dummy.set_upstream([task_nodes[n] for n in g.nodes() if g.out_degree(n) == 0])
            globals()[dag_id] = dag

_execute()
