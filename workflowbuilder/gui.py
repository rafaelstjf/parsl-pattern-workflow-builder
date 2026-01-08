import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import networkx as nx
from models import SingleTask, MapReduce, Fan, Workflow
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

class WorkflowGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Workflow Visual Editor")
        self.workflow = Workflow()

        self.create_widgets()
        self.update_pattern_list()
        self.draw_graph()

    def create_widgets(self):
        frame_controls = ttk.Frame(self.root)
        frame_controls.grid(row=0, column=0, sticky="ns", padx=10, pady=10)

        frame_patterns = ttk.LabelFrame(frame_controls, text="Padrões")
        frame_patterns.grid(row=0, column=0, pady=5)
        self.pattern_listbox = tk.Listbox(frame_patterns, width=25, height=15)
        self.pattern_listbox.grid(row=0, column=0, columnspan=2, padx=5, pady=5)
        ttk.Button(frame_patterns, text="Adicionar Padrão", command=self.add_pattern_window).grid(row=1, column=0, pady=5)
        ttk.Button(frame_patterns, text="Remover Padrão", command=self.remove_pattern_window).grid(row=1, column=1, pady=5)

        frame_edges = ttk.LabelFrame(frame_controls, text="Ligações")
        frame_edges.grid(row=1, column=0, pady=5)
        ttk.Button(frame_edges, text="Adicionar Ligação", command=self.add_edge_window).grid(row=0, column=0, pady=5)
        ttk.Button(frame_edges, text="Remover Ligação", command=self.remove_edge_window).grid(row=0, column=1, pady=5)

        frame_root = ttk.LabelFrame(frame_controls, text="Raiz")
        frame_root.grid(row=2, column=0, pady=5)
        ttk.Button(frame_root, text="Definir Raiz", command=self.set_root_window).grid(row=0, column=0, pady=5)

        frame_actions = ttk.LabelFrame(frame_controls, text="Ações")
        frame_actions.grid(row=3, column=0, pady=5)
        ttk.Button(frame_actions, text="Gerar Código", command=self.generate_code).grid(row=0, column=0, pady=5)
        ttk.Button(frame_actions, text="Salvar Workflow (.py)", command=self.save_workflow).grid(row=0, column=1, pady=5)
        

        # Frame para o grafo principal
        self.frame_graph = ttk.LabelFrame(self.root, text="Grafo de Padrões")
        self.frame_graph.grid(row=0, column=1, rowspan=4, padx=10, pady=10)
        self.fig, self.ax = plt.subplots(figsize=(6,6))
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.frame_graph)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

        # Conectar evento de clique
        self.canvas.mpl_connect("button_press_event", self.on_click_graph)

    def update_pattern_list(self):
        self.pattern_listbox.delete(0, tk.END)
        for pid in self.workflow.pattern_map.keys():
            root_mark = " (Raiz)" if pid == self.workflow.root_pid else ""
            self.pattern_listbox.insert(tk.END, pid + root_mark)

    def draw_graph(self):
        self.ax.clear()
        G = self.workflow.pattern_dag
        self.pos = nx.spring_layout(G)
        nx.draw(G, self.pos, ax=self.ax, with_labels=True, node_color="#E0E0FF",
                node_size=1500, font_size=10, arrowsize=20)
        self.ax.set_title("Grafo de Padrões")
        self.canvas.draw()

    # ---------- Clique interativo ----------
    def on_click_graph(self, event):
        if event.inaxes != self.ax:
            return
        # Encontrar nó mais próximo do clique
        min_dist = float("inf")
        closest_node = None
        for node, (x, y) in self.pos.items():
            dx = x - event.xdata
            dy = y - event.ydata
            dist = dx*dx + dy*dy
            if dist < min_dist:
                min_dist = dist
                closest_node = node
        if closest_node:
            self.show_pattern_dag(closest_node)

    def show_pattern_dag(self, pid):
        pattern = self.workflow.pattern_map.get(pid)
        if not pattern or not pattern.get_dag():
            messagebox.showinfo("DAG Padrão", f"O padrão '{pid}' não possui DAG interno.")
            return

        dag = pattern.get_dag()
        win = tk.Toplevel(self.root)
        win.title(f"DAG interno do padrão: {pid}")

        fig, ax = plt.subplots(figsize=(6,6))
        canvas = FigureCanvasTkAgg(fig, master=win)
        canvas.get_tk_widget().pack(fill="both", expand=True)

        pos = nx.spring_layout(dag)
        nx.draw(dag, pos, ax=ax, with_labels=True, node_color="#FFDDC1", node_size=1000,
                font_size=10, arrowsize=20)
        ax.set_title(f"DAG interno do padrão '{pid}'")
        canvas.draw()

    # ---------- Funções com Combobox ----------
    def add_pattern_window(self):
        win = tk.Toplevel(self.root)
        win.title("Adicionar Padrão")

        ttk.Label(win, text="Tipo de Padrão:").grid(row=0, column=0, padx=5, pady=5)
        pattern_type_cb = ttk.Combobox(win, values=["SingleTask", "MapReduce", "Fan"], state="readonly")
        pattern_type_cb.grid(row=0, column=1, padx=5, pady=5)
        pattern_type_cb.current(0)

        ttk.Label(win, text="ID do Padrão:").grid(row=1, column=0, padx=5, pady=5)
        pid_entry = ttk.Entry(win)
        pid_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(win, text="Tempo da Tarefa (s):").grid(row=2, column=0, padx=5, pady=5)
        time_entry = ttk.Entry(win)
        time_entry.grid(row=2, column=1, padx=5, pady=5)

        ttk.Label(win, text="Número de Tarefas (se aplicável):").grid(row=3, column=0, padx=5, pady=5)
        n_entry = ttk.Entry(win)
        n_entry.insert(0, "2")
        n_entry.grid(row=3, column=1, padx=5, pady=5)

        def add():
            try:
                pattern_type = pattern_type_cb.get()
                pid = pid_entry.get()
                time_ = int(time_entry.get())
                n = int(n_entry.get())
                if pattern_type == "SingleTask":
                    pattern = SingleTask(pid, time_)
                elif pattern_type == "MapReduce":
                    pattern = MapReduce(pid, n, time_)
                elif pattern_type == "Fan":
                    pattern = Fan(pid, n, time_)
                self.workflow.add_pattern(pattern)
                self.update_pattern_list()
                self.draw_graph()
                win.destroy()
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao adicionar padrão:\n{e}")

        ttk.Button(win, text="Adicionar", command=add).grid(row=4, column=0, columnspan=2, pady=10)

    def remove_pattern_window(self):
        win = tk.Toplevel(self.root)
        win.title("Remover Padrão")
        pids = list(self.workflow.pattern_map.keys())
        if not pids:
            messagebox.showerror("Erro", "Não há padrões para remover.")
            win.destroy()
            return

        ttk.Label(win, text="Escolha Padrão:").grid(row=0, column=0, padx=5, pady=5)
        cb_pid = ttk.Combobox(win, values=pids, state="readonly")
        cb_pid.grid(row=0, column=1, padx=5, pady=5)
        cb_pid.current(0)

        def remove():
            self.workflow.remove_pattern_node(cb_pid.get())
            self.update_pattern_list()
            self.draw_graph()
            win.destroy()

        ttk.Button(win, text="Remover", command=remove).grid(row=1, column=0, columnspan=2, pady=10)

    def add_edge_window(self):
        win = tk.Toplevel(self.root)
        win.title("Adicionar Ligação")
        pids = list(self.workflow.pattern_map.keys())
        if len(pids) < 2:
            messagebox.showerror("Erro", "É necessário ao menos 2 padrões.")
            win.destroy()
            return

        ttk.Label(win, text="Padrão Origem:").grid(row=0, column=0, padx=5, pady=5)
        cb_from = ttk.Combobox(win, values=pids, state="readonly")
        cb_from.grid(row=0, column=1, padx=5, pady=5)
        cb_from.current(0)

        ttk.Label(win, text="Padrão Destino:").grid(row=1, column=0, padx=5, pady=5)
        cb_to = ttk.Combobox(win, values=pids, state="readonly")
        cb_to.grid(row=1, column=1, padx=5, pady=5)
        cb_to.current(1 if len(pids) > 1 else 0)

        def add():
            self.workflow.add_pattern_edge(cb_from.get(), cb_to.get())
            self.draw_graph()
            win.destroy()

        ttk.Button(win, text="Adicionar Ligação", command=add).grid(row=2, column=0, columnspan=2, pady=10)

    def remove_edge_window(self):
        win = tk.Toplevel(self.root)
        win.title("Remover Ligação")
        edges = list(self.workflow.pattern_dag.edges)
        if not edges:
            messagebox.showerror("Erro", "Não há ligações para remover.")
            win.destroy()
            return

        ttk.Label(win, text="Escolha Ligação:").grid(row=0, column=0, padx=5, pady=5)
        edge_cb = ttk.Combobox(win, values=[f"{u} -> {v}" for u,v in edges], state="readonly")
        edge_cb.grid(row=0, column=1, padx=5, pady=5)
        edge_cb.current(0)

        def remove():
            u,v = edge_cb.get().split(" -> ")
            self.workflow.remove_pattern_edge(u, v)
            self.draw_graph()
            win.destroy()

        ttk.Button(win, text="Remover Ligação", command=remove).grid(row=1, column=0, columnspan=2, pady=10)

    def set_root_window(self):
        win = tk.Toplevel(self.root)
        win.title("Definir Raiz")
        pids = list(self.workflow.pattern_map.keys())
        if not pids:
            messagebox.showerror("Erro", "Não há padrões disponíveis.")
            win.destroy()
            return

        ttk.Label(win, text="Escolha Raiz:").grid(row=0, column=0, padx=5, pady=5)
        cb_root = ttk.Combobox(win, values=pids, state="readonly")
        cb_root.grid(row=0, column=1, padx=5, pady=5)
        cb_root.current(0)

        def set_root():
            self.workflow.set_new_root(cb_root.get())
            self.update_pattern_list()
            self.draw_graph()
            win.destroy()

        ttk.Button(win, text="Definir Raiz", command=set_root).grid(row=1, column=0, columnspan=2, pady=10)

    def generate_code(self):
        code = self.workflow.parse(save_pydot=False)
        CodeWindow(self.root, code)

    def save_workflow(self):
        code = self.workflow.parse(save_pydot=False)
        filepath = filedialog.asksaveasfilename(defaultextension=".py", filetypes=[("Python Files", "*.py")])
        if filepath:
            with open(filepath, "w") as f:
                f.write(code)
            messagebox.showinfo("Salvo", f"Workflow salvo em {filepath}.")
    # Função para exibir o grafo composto
    def show_composed_dag(self):
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
        dag_c = nx.DiGraph()

        # Inserir todos os subgrafos
        for pattern in self.workflow.pattern_map.values():
            dag = pattern.get_dag()
            if dag is not None:
                dag_c = nx.compose(dag_c, dag)

        # Conectar subgrafos conforme pattern_dag
        for u, v in self.workflow.pattern_dag.edges():
            pat_u = self.workflow.pattern_map[u]
            pat_v = self.workflow.pattern_map[v]
            for out_node in pat_u.get_outputs():
                for in_node in pat_v.get_inputs():
                    dag_c.add_edge(out_node, in_node)

        # Criar janela Tkinter
        win = tk.Toplevel(self.root)
        win.title("Grafo Composto Total (Todas as Tarefas)")
        
        fig, ax = plt.subplots(figsize=(8,8))
        pos = nx.spring_layout(dag_c)
        nx.draw(dag_c, pos, with_labels=True, node_color="#C1FFD7", node_size=1000, arrowsize=20, ax=ax)
        
        canvas = FigureCanvasTkAgg(fig, master=win)
        canvas.get_tk_widget().pack(fill="both", expand=True)
        canvas.draw()


class CodeWindow(tk.Toplevel):
    def __init__(self, master, code):
        super().__init__(master)
        self.title("Código Gerado")
        text = tk.Text(self, wrap="none", width=100, height=40)
        text.pack(fill="both", expand=True)
        text.insert("1.0", code)
        text.config(state="disabled")

if __name__ == "__main__":
    root = tk.Tk()
    app = WorkflowGUI(root)
    root.mainloop()
