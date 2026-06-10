import textwrap
import networkx as nx
import matplotlib.pyplot as plt


class Pattern():
    def __init__(self, id_):
        self.id_ = id_
        self.dag = None
        self.num_tasks = 0

    def get_num_tasks(self):
        return self.num_tasks

    def get_dag(self):
        return self.dag

    def get_id(self):
        return self.id_

    def get_source_nodes(self):
        if not self.dag or len(self.dag) == 0:
            return set()

        return {n for n in self.dag.nodes if self.dag.in_degree(n) == 0}

    def get_sink_nodes(self):
        if not self.dag or len(self.dag) == 0:
            return set()

        return {n for n in self.dag.nodes if self.dag.out_degree(n) == 0}


class SingleTask(Pattern):
    def __init__(self, id_, time_):
        super().__init__(id_)
        self.num_tasks = 1
        self.dag = nx.DiGraph()
        self.dag.add_node(str(self.id_) + "_0", time=time_)


class MapReduce(Pattern):
    def __init__(self, id_, n, time_):
        super().__init__(id_)
        self.dag = nx.DiGraph()
        self.num_tasks = n
        if isinstance(time_, list):
            if len(time_) == n:
                for i in range(0, n):
                    self.dag.add_node(str(self.id_) + "_" +
                                      str(i), time=time_[i])
        else:
            for i in range(0, n):
                self.dag.add_node(str(self.id_) + "_" + str(i), time=time_)
        for i in range(0, n-1):
            self.dag.add_edge(str(self.id_) + "_" + str(i),
                              str(self.id_) + "_" + str(n-1))


class Fan(Pattern):
    def __init__(self, id_, n, time_):
        super().__init__(id_)
        self.num_tasks = n
        self.dag = nx.DiGraph()
        if isinstance(time_, list):
            if len(time_) == n:
                for i in range(0, n):
                    self.dag.add_node(str(self.id_) + "_" +
                                      str(i), time=time_[i])
        else:
            for i in range(0, n):
                self.dag.add_node(str(self.id_) + "_" + str(i), time=time_)
        for i in range(1, n):
            self.dag.add_edge(str(self.id_) + "_" + str(0),
                              str(self.id_) + "_" + str(i))


class Pipeline(Pattern):
    def __init__(self, id_, n, time_):
        super().__init__(id_)
        self.dag = nx.DiGraph()
        self.num_tasks = n
        if isinstance(time_, list):
            if len(time_) == n:
                for i in range(0, n):
                    self.dag.add_node(str(self.id_) + "_" +
                                      str(i), time=time_[i])
        else:
            for i in range(0, n):
                self.dag.add_node(str(self.id_) + "_" + str(i), time=time_)
        for i in range(1, n):
            self.dag.add_edge(str(self.id_) + "_" + str(i-1),
                              str(self.id_) + "_" + str(i))


class Workflow():
    def __init__(self) -> None:
        self.pattern_map = dict()
        self.pattern_dag = nx.DiGraph()
        self.root_pid = None
        self.dag_parsed = None

    def add_pattern(self, pattern):
        p_id = pattern.get_id()
        if self.pattern_map.get(p_id) == None:
            self.pattern_map[p_id] = pattern
            self.pattern_dag.add_node(p_id)
            if self.root_pid == None:
                self.root_pid = p_id

    def add_pattern_edge(self, p_id1, p_id2):
        if not self.pattern_dag.has_edge(p_id1, p_id2):
            self.pattern_dag.add_edge(p_id1, p_id2)

    def set_new_root(self, p_id):
        if self.pattern_map.get(p_id) is not None:
            self.root_pid = p_id

    def remove_pattern_edge(self, p_id1, p_id2):
        self.pattern_dag.remove_edge(p_id1, p_id2)

    def remove_pattern_node(self, p_id):
        self.pattern_dag.remove_node(p_id)
        self.pattern_map.pop(p_id)

    def parse(self, bash_app=False, time_as_arg=False):
        if self.dag_parsed:
            self.dag_parsed.clear()
        self.dag_parsed = nx.DiGraph()
        # Insert all the patterns in the complete dag
        for pid, pattern in self.pattern_map.items():
            dag = pattern.get_dag()
            if dag is None or len(dag) == 0:
                continue
            self.dag_parsed = nx.compose(self.dag_parsed, dag)

        # Conect all the connected components using the sink nodes from u and the source nodes from v
        for (u, v) in self.pattern_dag.edges():
            pat_u = self.pattern_map[u]
            pat_v = self.pattern_map[v]

            for out_node in pat_u.get_sink_nodes():
                for in_node in pat_v.get_source_nodes():
                    u_out = out_node
                    v_in = in_node
                    self.dag_parsed.add_edge(u_out, v_in)

        task_definitions = []
        task_exec = []

        # Get the different execution times
        node_times = {n: self.dag_parsed.nodes[n].get(
            "time", 1) for n in self.dag_parsed.nodes}
        times = list(set(node_times.values()))
        if bash_app is False:
            if time_as_arg is False:
                for t in times:
                    # Define a function for each time, once that all tasks with the same time will use the same function
                    task_code = textwrap.dedent(f"""
                        @python_app()
                        def task_t{t}(inputs=[]):
                            import time
                            duration = {t}
                            end = time.time() + duration
                            while time.time() < end:
                                _ = 123456789 ** 2
                            return "done"
                    """).strip()
                    task_definitions.append(task_code)
            else:
                task_code = textwrap.dedent(f"""
                        @python_app()
                        def task_t(inputs=[], duration = 0):
                            import time
                            end = time.time() + duration
                            while time.time() < end:
                                _ = 123456789 ** 2
                            return "done"
                    """).strip()
                task_definitions.append(task_code)
        else:
            # if time_as_arg == False:
            for t in times:
                task_code = textwrap.dedent(f"""
                    @bash_app()
                    def task_t{t}(inputs=[]):
                        return 'end=$(( $(date +%s) + {t} )); while [ "$(date +%s)" -lt "$end" ]; do : $((123456789*123456789)); done; echo done'
                """).strip()
                task_definitions.append(task_code)
            # else:
            #     task_code = textwrap.dedent(f"""
            #         @bash_app()
            #         def task_t(inputs=[], duration = 0):
            #             return 'end=$(( $(date +%s) + $1 )); while [ "$(date +%s)" -lt "$end" ]; do : $((123456789*123456789)); done; echo done'
            #     """).strip()
            #     task_definitions.append(task_code)

        # Each node has a variable, the function call is based on the definitions generated earlier
        for n in self.dag_parsed.nodes:
            time_ = node_times[n]
            parents = list(self.dag_parsed.predecessors(n))
            params = "inputs=["
            if parents:
                params += ",".join(f"r_{p}" for p in parents)
            params += "]"
            if time_as_arg == False or bash_app == True:
                task_exec.append(f"r_{n} = task_t{time_}({params})")
            elif bash_app == False:  # only supported in python app
                task_exec.append(
                    f"r_{n} = task_t({params}, duration = {time_})")

        return "\n\n".join(task_definitions) + "\n\n", "\n".join(task_exec)

    def export_pydot(self, save_pydot=True, filename=None):
        if self.dag_parsed is not None:
            pydot_graph = nx.nx_pydot.to_pydot(self.dag_parsed)
            for node in pydot_graph.get_nodes():
                name = node.get_name().strip('"')
                data = self.dag_parsed.nodes.get(name, {})
                label = name
                if "time" in data:
                    label += f"\\n({data['time']}s)"
                node.set_label(label)
                node.set_shape("box")
                node.set_style("rounded,filled")
                node.set_fillcolor("#E0E0FF")

            if save_pydot == True:
                if filename == None:
                    filename = "workflow.pdf"
                pydot_graph.write(filename, format="pdf")
                print(f"DAG stored at: {filename}")
            print(pydot_graph.to_string())

        else:
            print("Error! the DAG was not parsed!")
