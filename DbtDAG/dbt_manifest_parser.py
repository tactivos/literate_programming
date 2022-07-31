from src.common.lib.constants import \
    (AIRFLOW_LOCAL_ROOT,
     DBT_GLOBAL_CLI_FLAGS,
     DBT_PROJECT_DIR,
     DBT_PROFILES_DIR,
     DBT_TARGET)
import matplotlib.pyplot as plt
import copy

import json
import networkx as nx
import os

#AIRFLOW_LOCAL_ROOT = '/Users/abhattacharya/data-jobs/'

def load_manifest():
    print(f'{AIRFLOW_LOCAL_ROOT}')
    with open(f'{AIRFLOW_LOCAL_ROOT}/dbt/target/manifest.json') as f:
        data = json.load(f)
    return data

class DbtManifestParser:
    def __init__(self,
                 dbt_global_cli_flags=DBT_GLOBAL_CLI_FLAGS,
                 dbt_project_dir=DBT_PROJECT_DIR,
                 dbt_profiles_dir=DBT_PROFILES_DIR,
                 dbt_target=DBT_TARGET,
                 dbt_project='mural_dbt'):
        self.data = load_manifest()
        self.g = nx.DiGraph()
        self.dbt_project = dbt_project
        self.dbt_global_cli_flags = dbt_global_cli_flags
        self.dbt_project_dir = dbt_project_dir
        self.dbt_target = dbt_target
        self.dbt_profiles_dir = dbt_profiles_dir
        self._build_nx_graph()

    def get_all_dag_names(self):
        return list(set(d['dag_name'] for n, d in self.g.nodes(data=True)))

    def get_dag(self, dag_name:str)->nx.DiGraph:
        return self.g.subgraph([n for n, d in self.g.nodes(data=True) if d['dag_name'] == dag_name])

    def _build_nx_graph(self):
        for k,v in self.data['nodes'].items():
            # Pick up models & snapshots (these will be task nodes in Airflow)
            (object_type, project) = k.split('.')[:2]
            if project != self.dbt_project or object_type not in ['model', 'snapshot']:
                continue

            if 'tags' in v and len(v['tags']) > 0 and v['tags'][0] in ['daily', 'monthly', 'weekly', 'hourly']:
                continue

            # Look for DAG name in meta tag (if present)
            # If it is not then use the directory as DAG name
            v.setdefault(v['config'].setdefault(v['config']['meta'].setdefault('dag_name', os.path.dirname(v['original_file_path']).split('/')[-1])))
            dag_name = v['config']['meta']['dag_name']
            # task name is same as DBT model name
            task_name = k.split('.')[-1]

            # Extract dependency. Dependency may change if the model has test(s) defined
            # Test(s) are considered as DQ checks.
            # Any downstream model dependency will be converted to dependency on upstream dq check
            # This is done to make the downstream task fail if DQ check for upstream task fails

            # Dependency list needs to be deduped
            # As upstream model name may appear more than once
            tasks = list(set([x.split('.')[-1] for x in v['depends_on']['nodes'] if x.split('.')[0] in ['model', 'snapshot']]))

            self.g.add_node(task_name,
                            dag_name=dag_name,
                            operator='BashOperator',
                            command=f'\
                dbt run --target {self.dbt_target} --models {task_name} \
                --profiles-dir {self.dbt_profiles_dir} --project-dir {self.dbt_project_dir}',
                            type='model',
                            dq_task_name=None,
                            upstream_tasks=tasks if len(tasks) > 0 else [])

        for k,v in self.data['child_map'].items():
            (object_type, project) = k.split('.')[:2]
            if project != self.dbt_project or object_type not in ['model', 'snapshot']:
                continue

            upstream_task_name = k.split('.')[-1]
            if upstream_task_name not in self.g.nodes():
                continue

            dq_task_exists = len([x for x in v if x.split('.')[0] == 'test']) > 0

            # Upstream task name as extracted from child map
            dag_name = self.g.nodes[upstream_task_name]['dag_name']
            if dq_task_exists: # Data quality task name
                dq_task_name = 'dq_' + upstream_task_name
                self.g.add_node(
                    dq_task_name,
                    dag_name=dag_name,
                    operator='BashOperator',
                    type='test',
                    command=f'\
                dbt test --target {self.dbt_target} --models {upstream_task_name} \
                --profiles-dir {self.dbt_profiles_dir} --project-dir {self.dbt_project_dir}',
                    dq_task_name=None,
                    upstream_tasks=[upstream_task_name])

                self.g.add_edge(upstream_task_name, dq_task_name)
                self.g.nodes[upstream_task_name]['dq_task_name'] = dq_task_name

        # Deepy copy the graph as new nodes will be added
        # while looping through existing nodes
        for n, d in copy.deepcopy(self.g.nodes(data=True)):
            # Pick up only model entries as their dependencies may have to modified
            if d['type'] != 'model':
                continue
            for upstream_task in d['upstream_tasks']:
                dq_task_name = self.g.nodes[upstream_task]['dq_task_name']
                if dq_task_name is not None:
                    # Include wait-for task if upstream and dowstream tasks belong to two DAGs
                    if self.g.nodes[upstream_task]['dag_name'] != d['dag_name']:
                        wait_for_task_name = 'wf_' + dq_task_name
                        self.g.add_node(
                            wait_for_task_name,
                            dag_name=d['dag_name'],
                            operator='sensor',
                            type='wait_for')
                        self.g.add_edge(wait_for_task_name, n)
                        self.g.add_edge(dq_task_name, wait_for_task_name)
                    else:
                        self.g.add_edge(dq_task_name, n)
                else:
                    self.g.add_edge(upstream_task, n)

    def draw_graph(self, graph):
        nx.draw(graph,
                with_labels=True,
                edge_color='black',
                width=2,
                linewidths=1,
                node_size=100,
                node_color='blue',
                alpha=0.5)
        plt.show()

if __name__ == '__main__':
    dbt_parser = DbtManifestParser()
    for g in dbt_parser.get_all_dag_names():
        dbt_parser.draw_graph(dbt_parser.get_dag(g))


