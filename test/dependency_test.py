import unittest
import networkx as nx
import matplotlib.pyplot as plt


# 假设这是你的依赖图构建逻辑
def build_dependency_graph():
    G = nx.DiGraph()
    # 示例数据：添加节点和边到图中
    G.add_edges_from([('table1', 'table2'), ('table2', 'table3')])
    return G


class TestDependencyGraph(unittest.TestCase):

    def setUp(self):
        self.graph = build_dependency_graph()

    def test_build_dependency_graph(self):
        # 测试图是否正确建立
        self.assertTrue('table1' in self.graph.nodes)
        self.assertTrue(('table1', 'table2') in self.graph.edges)

    def test_topological_sort(self):
        # 检查拓扑排序是否可行（无环）
        try:
            list(nx.topological_sort(self.graph))
        except nx.NetworkXUnfeasible:
            self.fail("Graph has a cycle, cannot do topological sort.")

    def draw_dependency_graph(self):
        # 绘制依赖图
        plt.figure(figsize=(8, 6))
        nx.draw(self.graph, with_labels=True, node_color='lightblue', arrows=True, node_size=2000, font_size=16)
        plt.title("Table Dependency Graph")
        plt.show()


# 添加一个主函数来运行测试并显示图表
if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDependencyGraph)
    unittest.TextTestRunner(verbosity=2).run(suite)

    # 单独调用绘图方法，以显示图形化结果
    graph = build_dependency_graph()
    plt.figure(figsize=(8, 6))
    nx.draw(graph, with_labels=True, node_color='lightblue', arrows=True, node_size=2000, font_size=16)
    plt.title("Table Dependency Graph")
    plt.show()
