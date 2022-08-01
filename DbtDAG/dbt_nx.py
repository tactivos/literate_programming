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
    subproccess_deps = _refresh_deps()
    if subproccess_deps.returncode == 0:
        logging.info('DBT packages successfully refreshed')

    subproccess_compile = _refresh_manifest()
    if subproccess_compile.returncode == 0:
        logging.info('DBT manifest file successfully refreshed')

    dbt_manifest_parser = DbtManifestParser()
    for dag_id in dbt_manifest_parser.get_all_dags():

        dag: DAG = airflow_dag.create(
            execution_timeout=timedelta(seconds=60),
            description="DAG generated from DBT manifest file",
            dag_owner=airflow_dag.DAGOwner.data_platform,
            dag_id=dag_id,
            start_date=datetime(year=2022, month=7, day=29,
                                hour=0, minute=0, second=0),
            schedule_interval="@daily"
        )

        with dag:
            g = dbt_manifest_parser.get_dag(dag_id)

            task_nodes = {}

            # Add task nodes to Airflow DAG
            for node, data in g.nodes(data=True):
                if data['type'] in ['model', 'snapshot', 'test']:
                    task_nodes[node] = BashOperator(
                        task_id=node,
                        bash_command=data['command'],
                        dag=dag,
                    )
            # Add wait-for node to Airflow DAG
            for edge in g.edges():
                for i in range(2):
                    if g.nodes[edge[i]]['type'] == 'wait_for':
                        wait_for_node = edge[i]
                        if edge[i] not in task_nodes:
                            external_dag = g.nodes[wait_for_node]['external_dag']
                            external_task = g.nodes[wait_for_node]['external_task']
                            task_nodes[edge[i]] = wait_for(
                                dag,
                                task_id=wait_for_node,
                                external_dag_id=external_dag,
                                external_task_id=external_task
                            )

            for edge in g.edges():
                task_nodes[edge[0]].set_downstream(task_nodes[edge[1]])

            start_dummy = DummyOperator(task_id='start')
            end_dummy = DummyOperator(task_id='end')
            # Connect start node to all task nodes having indegree = 0
            start_dummy.set_downstream([task_nodes[n] for n in g.nodes() if g.in_degree(n) == 0])
            # Connect end code to all task nodes having outdegree = 0
            end_dummy.set_upstream([task_nodes[n] for n in g.nodes() if g.out_degree(n) == 0])
            globals()[dag_id] = dag

_execute()