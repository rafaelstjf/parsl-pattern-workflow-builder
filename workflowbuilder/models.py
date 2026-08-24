import textwrap
import re
import networkx as nx
import matplotlib.pyplot as plt



class Pattern():
    """
    @brief Base class representing a workflow pattern.

    A pattern encapsulates a directed acyclic graph (DAG) representing a
    reusable workflow structure. Derived classes implement specific
    communication patterns such as pipelines, fan-out, and map-reduce.
    """
    def __init__(self, id_):
        self.id_ = id_
        self.dag = None
        self.num_tasks = 0

    def get_num_tasks(self) -> int:
        """
        @brief Returns the number of tasks in the pattern.

        @return Number of tasks.
        """
        return self.num_tasks

    def get_dag(self):
        """
        @brief Returns the DAG associated with the pattern.

        @return A NetworkX directed graph.
        """
        return self.dag

    def get_id(self):
        """
        @brief Returns the pattern identifier.

        @return Pattern identifier.
        """
        return self.id_

    def get_source_nodes(self):
        """
        @brief Returns all source nodes of the pattern.

        Source nodes are those with zero incoming edges.

        @return Set containing the source nodes.
        """
        if not self.dag or len(self.dag) == 0:
            return set()

        return {n for n in self.dag.nodes if self.dag.in_degree(n) == 0}

    def get_sink_nodes(self):
        """
        @brief Returns all sink nodes of the pattern.

        Sink nodes are those with zero outgoing edges.

        @return Set containing the sink nodes.
        """
        if not self.dag or len(self.dag) == 0:
            return set()

        return {n for n in self.dag.nodes if self.dag.out_degree(n) == 0}


class SingleTask(Pattern):
    """
    @brief Pattern composed of a single task.
    """
    def __init__(self, id_, time_):
        """
        @brief Creates a single-task pattern.

        @param id_ Pattern identifier.
        @param time_ Execution time assigned to the task.
        """
        super().__init__(id_)
        self.num_tasks = 1
        self.dag = nx.DiGraph()
        self.dag.add_node(str(self.id_) + "_0", time=time_)


class MapReduce(Pattern):
    """
    @brief Implements a MapReduce workflow pattern.

    The last task acts as the reduce stage and depends on every preceding
    map task.
    """
    def __init__(self, id_, n, time_):
        """
        @brief Creates a MapReduce pattern.

        @param id_ Pattern identifier.
        @param n Number of tasks.
        @param time_ Execution time for each task or a list containing the
                    execution time of every task.
        """
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
    """
    @brief Implements a fan-out workflow pattern.

    The first task precedes all remaining tasks.
    """
    def __init__(self, id_, n, time_):
        """
        @brief Creates a fan-out pattern.

        @param id_ Pattern identifier.
        @param n Number of tasks.
        @param time_ Execution time for each task or a list containing the
                    execution time of every task.
    """
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
    """
    @brief Implements a pipeline workflow pattern.

    Tasks are connected sequentially.
    """
    def __init__(self, id_, n, time_):
        """
        @brief Creates a pipeline pattern.

        @param id_ Pattern identifier.
        @param n Number of tasks.
        @param time_ Execution time for each task or a list containing the
                    execution time of every task.
        """
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
    """
    @brief Builds and manipulates workflows composed of multiple patterns.

    A workflow is represented as a DAG whose vertices are tasks belonging to
    individual patterns. Patterns are connected according to configurable
    connection policies and can later be exported or translated into Parsl
    applications.
    """
    def __init__(self, wms="parsl") -> None:
        self.pattern_map = dict()
        self.pattern_dag = nx.DiGraph()
        self.root_pid = None
        self.dag_parsed = None
        self.wms = wms

    def add_pattern(self, pattern) -> None:
        """
        @brief Adds a pattern to the workflow.

        @param pattern Pattern instance to be inserted.

        @return None
        """
        p_id = pattern.get_id()
        if self.pattern_map.get(p_id) == None:
            self.pattern_map[p_id] = pattern
            self.pattern_dag.add_node(p_id)
            if self.root_pid == None:
                self.root_pid = p_id

    def add_pattern_edge(self, p_id1, p_id2, policy="all_to_all", source_index=None, target_index=None) -> None:
        """
        @brief Creates a dependency between two patterns.

        @param p_id1 Identifier of the source pattern.
        @param p_id2 Identifier of the target pattern.
        @param policy Connection policy.
        @param source_index Optional sink node index in the source pattern.
        @param target_index Optional source node index in the target pattern.

        @return None
        """
        self.pattern_dag.add_edge(
            p_id1,
            p_id2,
            policy=policy,
            source_index=source_index,
            target_index=target_index,
        )
        
    def _node_sort_key(self, node):
        return [
            int(part) if part.isdigit() else part
            for part in re.split(r"(\d+)", str(node))
        ]

    def _select_node_by_index(self, nodes, pattern_id, index, node_kind):
        if index is None:
            return None

        ordered_nodes = sorted(nodes, key=self._node_sort_key)
        if isinstance(index, int) and 0 <= index < len(ordered_nodes):
            return ordered_nodes[index]

        composed_id = f"{pattern_id}_{index}"
        return next((node for node in ordered_nodes if node == composed_id), None)

    def _connect_pattern_nodes(self, pat_u, pat_v, policy="all_to_all", source_index=None, target_index=None):
        sink_nodes = sorted(pat_u.get_sink_nodes(), key=self._node_sort_key)
        source_nodes = sorted(pat_v.get_source_nodes(), key=self._node_sort_key)

        if not sink_nodes or not source_nodes:
            return

        if policy == "all_to_all":
            for out_node in sink_nodes:
                for in_node in source_nodes:
                    self.dag_parsed.add_edge(out_node, in_node)
            return

        if policy == "one_to_one":
            if len(sink_nodes) != len(source_nodes):
                raise ValueError(
                    f"Cannot connect '{pat_u.get_id()}' to '{pat_v.get_id()}' with one_to_one policy: "
                    f"{len(sink_nodes)} sink nodes and {len(source_nodes)} source nodes."
                )
            for out_node, in_node in zip(sink_nodes, source_nodes):
                self.dag_parsed.add_edge(out_node, in_node)
            return

        if policy == "target_index":
            if target_index is None:
                raise ValueError("target_index policy requires target_index.")

            target_node = self._select_node_by_index(
                source_nodes,
                pat_v.get_id(),
                target_index,
                "source",
            )

            if target_node is None:
                raise ValueError(
                    f"target_index {target_index} does not match any source node for pattern '{pat_v.get_id()}'."
                )

            if source_index is None:
                for source_node in sink_nodes:
                    self.dag_parsed.add_edge(source_node, target_node)
            else:
                source_node = self._select_node_by_index(
                    sink_nodes,
                    pat_u.get_id(),
                    source_index,
                    "sink",
                )

                if source_node is None:
                    raise ValueError(
                        f"source_index {source_index} does not match any sink node for pattern '{pat_u.get_id()}'."
                    )

                self.dag_parsed.add_edge(source_node, target_node)
            return

        raise ValueError(f"Unknown connection policy: {policy}")

    def set_new_root(self, p_id):
        """
        @brief Sets a new root pattern.

        @param p_id Identifier of the new root pattern.

        @return None
        """
        if self.pattern_map.get(p_id) is not None:
            self.root_pid = p_id

    def remove_pattern_edge(self, p_id1, p_id2):
        """
        @brief Removes a dependency between two patterns.

        @param p_id1 Identifier of the source pattern.
        @param p_id2 Identifier of the target pattern.

        @return None
        """
        self.pattern_dag.remove_edge(p_id1, p_id2)

    def remove_pattern_node(self, p_id):
        """
        @brief Removes a pattern from the workflow.

        The pattern and all of its connections are removed.

        @param p_id Identifier of the pattern.

        @return None
        """
        self.pattern_dag.remove_node(p_id)
        self.pattern_map.pop(p_id)

    def __parse_to_parsl(self, bash_app=False, time_as_arg=False):
        task_definitions = []
        task_exec = []
        # Get the different execution times
        node_times = {n: self.dag_parsed.nodes[n].get(
            "time", 1) for n in self.dag_parsed.nodes}
        times = list(dict.fromkeys(node_times.values()))
        task_names = {
            time_: f"task_t_{index}"
            for index, time_ in enumerate(times)
        }
        if bash_app is False:
            if time_as_arg is False:
                for t in times:
                    task_name = task_names[t]
                    # Define a function for each time, once that all tasks with the same time will use the same function
                    task_code = textwrap.dedent(f"""
                        @python_app()
                        def {task_name}(inputs=[]):
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
                task_name = task_names[t]
                task_code = textwrap.dedent(f"""
                    @bash_app()
                    def {task_name}(inputs=[]):
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
        for n in nx.topological_sort(self.dag_parsed):
            time_ = node_times[n]
            parents = list(self.dag_parsed.predecessors(n))
            params = "inputs=["
            if parents:
                params += ",".join(f"r_{p}" for p in parents)
            params += "]"
            if time_as_arg == False or bash_app == True:
                task_exec.append(
                    f"r_{n} = {task_names[time_]}({params})"
                )
            elif bash_app == False:  # only supported in python app
                task_exec.append(
                    f"r_{n} = task_t({params}, duration = {time_})")

        return "\n\n".join(task_definitions) + "\n\n", "\n".join(task_exec)

    def __parse_to_pycompss(self, bash_app=False, time_as_arg=False):
        task_definitions = []
        task_exec = []

        # Get the different execution times
        node_times = {
            n: self.dag_parsed.nodes[n].get("time", 1)
            for n in self.dag_parsed.nodes
        }

        times = list(dict.fromkeys(node_times.values()))
        task_names = {
            time_: f"task_t_{index}"
            for index, time_ in enumerate(times)
        }

        if bash_app is False:
            if time_as_arg is False:
                for t in times:
                    task_name = task_names[t]
                    # Define a function for each time, since all tasks with the
                    # same time will use the same function
                    task_code = textwrap.dedent(f"""
                        @task(inputs=COLLECTION_IN, returns=str)
                        def {task_name}(inputs=[]):
                            import time
                            duration = {t}
                            end = time.time() + duration
                            while time.time() < end:
                                _ = 123456789 ** 2
                            return "done"
                    """).strip()

                    task_definitions.append(task_code)
            else:
                task_code = textwrap.dedent("""
                    @task(inputs=COLLECTION_IN, returns=str)
                    def task_t(inputs=[], duration=0):
                        import time
                        end = time.time() + duration
                        while time.time() < end:
                            _ = 123456789 ** 2
                        return "done"
                """).strip()

                task_definitions.append(task_code)

        else:
            # TODO: Add support to pycompss' binary
            return None

        # Each node has a variable; the function call is based on the
        # definitions generated earlier
        for n in nx.topological_sort(self.dag_parsed):
            time_ = node_times[n]
            parents = list(self.dag_parsed.predecessors(n))

            params = "inputs=["

            if parents:
                params += ",".join(f"r_{p}" for p in parents)

            params += "]"

            if time_as_arg is False or bash_app is True:
                task_exec.append(
                    f"r_{n} = {task_names[time_]}({params})"
                )
            elif bash_app is False:
                task_exec.append(
                    f"r_{n} = task_t({params}, duration={time_})"
                )

        return (
            "\n\n".join(task_definitions) + "\n\n",
            "\n".join(task_exec)
        )
    
    def parse(self, bash_app=False, time_as_arg=False):
        """
        @brief Generates the complete workflow DAG and the corresponding Parsl code.

        This method composes all pattern DAGs, connects them according to their
        configured policies, and generates the task definitions and execution
        statements.

        @param bash_app Generate Bash applications instead of Python applications.
        @param time_as_arg Pass the execution time as a task argument instead of
                        generating one function per execution time.

        @return A tuple containing:
                - The generated task definitions.
                - The generated workflow execution code.
        """
        if self.dag_parsed:
            self.dag_parsed.clear()
        self.dag_parsed = nx.DiGraph()
        # Insert all the patterns in the complete dag
        for pid, pattern in self.pattern_map.items():
            dag = pattern.get_dag()
            if dag is None or len(dag) == 0:
                continue
            self.dag_parsed = nx.compose(self.dag_parsed, dag)

        # Connect pattern edges using the configured policy for each edge.
        for (u, v, edge_data) in self.pattern_dag.edges(data=True):
            pat_u = self.pattern_map[u]
            pat_v = self.pattern_map[v]
            self._connect_pattern_nodes(
                pat_u,
                pat_v,
                policy=edge_data.get("policy", "all_to_all"),
                source_index=edge_data.get("source_index"),
                target_index=edge_data.get("target_index"),
            )
        
        if self.wms == "parsl":
            return self.__parse_to_parsl(bash_app, time_as_arg)
        elif self.wms == "pycompss":
            return self.__parse_to_pycompss(bash_app, time_as_arg)
        else:
            print("WMS not supported for parsing!")
            return None

    def export_pydot(self, save_pydot=True, filename=None):
        """
        @brief Exports the parsed workflow DAG as a Graphviz graph.

        Optionally saves the graph as a PDF file.

        @param save_pydot If True, writes the graph to a file.
        @param filename Output filename. If None, "workflow.pdf" is used.

        @return None
        """
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
