import grpc
from concurrent.futures import ThreadPoolExecutor
import raft_pb2
import raft_pb2_grpc
import threading
import time
import socket
import os
import random

class LogEntry:
    def __init__(self, term, operation):
        self.term = term
        self.operation = operation

class RaftNode(raft_pb2_grpc.RaftNodeServicer):
    def __init__(self, node_id):
        self.node_id = node_id
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {}
        self.match_index = {}
        self.leader_id = None
        self.state = 'follower'
        self.election_timer = None
        self.heartbeat_timer = None
        self.leader_lease_timer = None
        self.client_stub = None
        self.server_stubs = {}
        self.clients = []

        # Create directory for logs and metadata
        self.logs_directory = f'logs_node_{node_id}'
        os.makedirs(self.logs_directory, exist_ok=True)

        # Load logs and metadata
        self.load_logs()
        self.load_metadata()

    def load_logs(self):
        log_file = os.path.join(self.logs_directory, 'logs.txt')
        if os.path.exists(log_file):
            with open(log_file, 'r') as file:
                for line in file:
                    parts = line.split()
                    term = int(parts[0])
                    operation = ' '.join(parts[1:])
                    log_entry = LogEntry(term, operation)
                    self.log.append(log_entry)

    def save_logs(self):
        log_file = os.path.join(self.logs_directory, 'logs.txt')
        with open(log_file, 'w') as file:
            for entry in self.log:
                file.write(f'{entry.term} {entry.operation}\n')

    def load_metadata(self):
        metadata_file = os.path.join(self.logs_directory, 'metadata.txt')
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as file:
                lines = file.readlines()
                if len(lines) >= 3:
                    self.current_term = int(lines[0].strip())
                    voted_for_line = lines[1].strip()
                    if voted_for_line and voted_for_line != 'None':
                        self.voted_for = int(voted_for_line)
                    self.commit_index = int(lines[2].strip())

    def save_metadata(self):
        metadata_file = os.path.join(self.logs_directory, 'metadata.txt')
        with open(metadata_file, 'w') as file:
            file.write(f'{self.current_term}\n')
            file.write(f'{self.voted_for}\n')
            file.write(f'{self.commit_index}\n')

    def RequestVote(self, request, context):
        response = raft_pb2.RequestVoteResponse()
        candidate_term = request.term
        candidate_id = request.candidate_id
        last_log_index = request.last_log_index
        last_log_term = request.last_log_term

        # Check if the candidate's term is greater than the current term
        if candidate_term > self.current_term:
            if last_log_term > self.log[-1].term or \
               (last_log_term == self.log[-1].term and last_log_index >= len(self.log) - 1):
                # Grant vote if candidate's log is at least as up-to-date as receiver's log
                self.current_term = candidate_term
                self.voted_for = candidate_id
                response.vote_granted = True
            else:
                response.vote_granted = False
        else:
            response.vote_granted = False

        # Set the current term in the response
        response.term = self.current_term
        return response

    def AppendEntry(self, request, context):
        response = raft_pb2.AppendEntryResponse()
        leader_term = request.term
        leader_id = request.leader_id
        prev_log_index = request.prev_log_index
        prev_log_term = request.prev_log_term

        # Check if the leader's term is greater than or equal to the current term
        if leader_term >= self.current_term:
            if prev_log_index >= len(self.log) or self.log[prev_log_index].term != prev_log_term:
                # Reject the entry if the log doesn't match the previous log entry
                response.success = False
            else:
                # Append new log entries
                for entry in request.entries:
                    if entry.index >= len(self.log) or self.log[entry.index].term != entry.term:
                        self.log[entry.index] = entry

                # Update commit index and apply committed entries to state machine
                self.commit_index = min(request.leader_commit, len(self.log) - 1)
                self.last_applied = self.commit_index
                response.success = True
        else:
            # Reject the entry if the leader's term is less than the current term
            response.success = False
        
        return response

    def RequestVoteRPC(self):
        server = grpc.server(thread_pool=ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftNodeServicer_to_server(self, server)
        server.add_insecure_port('[::]:50051')
        server.start()
        server.wait_for_termination()

    def AppendEntryRPC(self):
        for node_id in self.server_stubs:
            if node_id != self.node_id:  # Skip the current node
                stub = self.server_stubs[node_id]
                prev_log_index = self.next_index[node_id] - 1
                prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else -1
                request = raft_pb2.AppendEntryRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    leader_commit=self.commit_index,
                    entries=self.log[self.next_index[node_id]:]
                )
                response = stub.AppendEntry(request)
                # Process response
                if response.success:
                    self.match_index[node_id] = prev_log_index + len(request.entries)
                    self.next_index[node_id] = self.match_index[node_id] + 1
                else:
                    self.next_index[node_id] -= 1

    def handle_client_requests(self):
        while True:
            request = input("Enter a client request: ")
            if request.startswith("SET"):
                _, key, value = request.split()
                log_entry = LogEntry(term=self.current_term, operation=f"SET {key} {value}")
                self.log.append(log_entry)
                print(f"SET operation added to log: {key}={value}")
                self.save_logs()
            elif request.startswith("GET"):
                _, key = request.split()
                latest_value = None
                for entry in reversed(self.log):
                    if entry.operation.startswith(f"SET {key}"):
                        latest_value = entry.operation.split()[2]
                        break
                if latest_value is not None:
                    print(f"GET operation: {key}={latest_value}")
                else:
                    print(f"No value found for key: {key}")
            else:
                print("Invalid request format. Please use SET or GET requests.")
    def create_dump(self):
        dump_file = os.path.join(self.logs_directory, 'dump.txt')
        with open(dump_file, 'w') as file:
            file.write(f'Node ID: {self.node_id}\n')
            file.write(f'Current Term: {self.current_term}\n')
            file.write(f'Voted For: {self.voted_for}\n')
            file.write(f'Commit Index: {self.commit_index}\n')
            file.write(f'Last Applied: {self.last_applied}\n')
            file.write(f'Next Index: {self.next_index}\n')
            file.write(f'Match Index: {self.match_index}\n')
            file.write(f'Leader ID: {self.leader_id}\n')
            file.write(f'State: {self.state}\n')
    def start_election(self):
        self.reset_election_timer()
        self.state = 'candidate'
        self.current_term += 1
        self.voted_for = self.node_id
        self.save_metadata()

        # Send RequestVote RPCs to other nodes
        for node_id, stub in self.server_stubs.items():
            if node_id != self.node_id:
                last_log_index = len(self.log) - 1
                last_log_term = self.log[last_log_index].term if last_log_index >= 0 else -1
                request = raft_pb2.RequestVoteRequest(
                    term=self.current_term,
                    candidate_id=self.node_id,
                    last_log_index=last_log_index,
                    last_log_term=last_log_term
                )
                response = stub.RequestVote(request)
                # Process response
                if response.term > self.current_term:
                    self.current_term = response.term
                    self.state = 'follower'
                    self.voted_for = None
                    self.save_metadata()
                    return
                elif response.vote_granted:
                    self.match_index[node_id] = 0
                    self.next_index[node_id] = len(self.log)

        # Check if received votes are the majority
        if sum(1 for node_id in self.match_index if node_id != self.node_id and self.match_index[node_id] >= len(self.log)) >= len(self.server_stubs) / 2:
            self.state = 'leader'
            self.leader_id = self.node_id
            self.leader_lease_timer = threading.Timer(5, self.start_election)
            self.leader_lease_timer.start()
            print(f"Node {self.node_id} became the leader.")
            self.send_heartbeat()
        else:
            self.reset_election_timer()

    def reset_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        timeout = random.randint(150, 300) / 1000  # Timeout between 150ms and 300ms
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start()

    def send_heartbeat(self):
        while self.state == 'leader':
            self.AppendEntryRPC()
            time.sleep(0.1)

    def __del__(self):
        # Save metadata before deleting the node
        self.save_metadata()
        print(f"Node {self.node_id} metadata saved before deletion.")

if __name__ == "__main__":
    # Example usage with multiple nodes
    num_nodes = 4  # Change this number to adjust the number of nodes
    nodes = []

    # Create and start each node in a separate thread
    for node_id in range(num_nodes):
        node = RaftNode(node_id)
        nodes.append(node)
        threading.Thread(target=node.RequestVoteRPC).start()

    # Start handling client requests for each node in a separate thread
    for node in nodes:
        threading.Thread(target=node.handle_client_requests).start()

    # Simulate appending logs for one of the nodes
    while True:
        time.sleep(random.uniform(0.5, 2))
        node = random.choice(nodes)  # Select a random node to simulate log appending
        operation = random.choice(["SET", "GET"])
        if operation == "SET":
            key = random.choice(["name1", "name2", "name3"])
            value = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=5))
            log_entry = LogEntry(term=node.current_term, operation=f"SET {key} {value}")
            node.log.append(log_entry)
            print(f"SET operation added to log of Node {node.node_id}: {key}={value}")
            node.save_logs()
            # Update metadata and dump.txt after every SET operation
            node.save_metadata()
            node.create_dump()
        elif operation == "GET":
            key = random.choice(["name1", "name2", "name3"])
            print(f"Simulating GET operation for key: {key}")

