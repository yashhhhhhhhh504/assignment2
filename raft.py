import raft_node
import threading

class Raft:
    def __init__(self, num_nodes):
        self.nodes = [raft_node.RaftNode(node_id) for node_id in range(num_nodes)]

    def run(self):
        threads = []
        for node in self.nodes:
            thread = threading.Thread(target=node.run)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

if __name__ == '__main__':
    num_nodes = 5
    raft_cluster = Raft(num_nodes)
    raft_cluster.run()
