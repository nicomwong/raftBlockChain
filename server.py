
import queue
from collections import defaultdict

import socket
import threading
import sys
import os
import time
import pprint

from DictServer import *


class Server:

    # Testing vars
    debugMode = False

    # Class vars
    basePort = 8000
    numServers = 3
    electionTimeout = 6 # Timeout to wait for majority promises after broadcasting prepare
    replicationTimeout = 6  # Timeout to wait for majority accepted after broadcasting accept (leader-only)

    def __init__(self, serverID):
        cls = self.__class__

        # My address
        self.ID = serverID
        self.ip = socket.gethostbyname(socket.gethostname() )
        self.port = cls.basePort + self.ID

        # Backup file
        self.backupBlockchainFileName = f"server{self.ID}_blockchain"

        # Simulation variables
        self.propagationDelay = 2
        self.brokenLinks = set()

        # Data structures
        self.blockchain = Blockchain()
        self.kvstore = KVStore()
        self.requestQueue = queue.Queue()  # Queue of blocks to propose when leader

        # Main Raft variables
        self.currentTerm = 0    # latest term server has seen
        self.votedFor = None    # candidateID that received vote in current term
        self.commitIndex = 0    # index of highest log entry known to be committed
        self.lastApplied = 0    # index of highest log entry applied to state machine
        self.state = ServerState.FOLLOWER   # state of server (initialized to follower)

        # Communication with client
        self.nominatorAddress = None    # client who nominated server to be leader
        self.leaderHintAddress = (socket.gethostbyname(socket.gethostname() ), cls.basePort + 1)  # Default hint is Server 1

        # Replication phase variables
        self.myVal = None

        # Get server addresses
        self.serverAddresses = []
        for i in range(cls.numServers):
            serverPort = cls.basePort + 1 + ( (self.ID + i) % cls.numServers)
            serverAddr = (socket.gethostbyname(socket.gethostname() ), serverPort)
            self.serverAddresses.append(serverAddr)
            # print("Added outgoing server address", serverAddr)

        # Leader variables (reinitialized after election)
        self.nextIndex = {server: self.blockchain.depth for server in self.serverAddresses} # for each server, index of the next log entry to send to that server
        self.matchIndex = {server: 0 for server in self.serverAddresses}    # for each server, index of highest log entry known to be replicated on server

    def start(self):
        # Setup my socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((socket.gethostname(), self.port))
        self.printLog(f"Started server at port {self.port}")

        # Recover from stable storage (save file) if there is one
        saveFileName = f"server{self.ID}_blockchain"
        if os.path.isfile(saveFileName):
            self.blockchain = Blockchain.read(saveFileName)
            self.kvstore = self.blockchain.generateKVStore()

        # Concurrently handle receiving messages
        threading.Thread(target=self.handleIncomingMessages, daemon=True).start()

        threading.Thread(target=self.processBlockQueue, daemon=True).start()

    def cleanExit(self):
        "Cleanly exits by closing all open sockets and files"
        self.sock.close()

    def electionPhase(self):
        cls = self.__class__

        # TODO broadcast "I am leader" to other servers if the election is successful

    def processBlockQueue(self):
        while True:
            if self.isLeader:
                if not self.requestQueue.empty():
                    currRequest = self.requestQueue.get()
                    if not self.blockchain._list:   # Blockchain is empty, so no previous block
                        prevBlock = None
                    else:
                        prevBlock = self.blockchain._list[-1]

                    self.myVal = Block.Create(currRequest[0], currRequest[1], prevBlock)
                    self.replicationPhase()
            else:
                # Flushes requestQueue, clients will handling resending any unanswered requests
                self.requestQueue.queue.clear()

    def replicationPhase(self):
        cls = self.__class__

    def sendMessage(self, msgTokens, destinationAddr):
        """
        Sends a message with components msgTokens to destinationAddr with a simulated self.propagationDelay second delay (Non-blocking).
        If the link to destinationAddr is broken, then nothing will arrive.
        """
        cls = self.__class__

        msgTokenStrings = [str(token) for token in msgTokens]  # Convert tokens to strings
        msg = '-'.join(msgTokenStrings)  # Separate tokens by delimiter

        if cls.debugMode:
            print(f"Sent message \"{msg}\" to machine at port {destinationAddr[1]}")

        if destinationAddr[1] in self.brokenLinks:
            # For simulating broken links, the message is sent but never arrives
            return

        threading.Thread(target=self._sendMessageWithDelay, args=(msg, destinationAddr), daemon=True).start()

    def _sendMessageWithDelay(self, msg, destinationAddr):
        "Only meant to be used in conjunction with self.sendMessage() to unblock with simulated delay"
        time.sleep(self.propagationDelay)
        self.sock.sendto(msg.encode(), destinationAddr)

    def broadcastToServers(self, *msgTokens, me=True):
        "Broadcasts msgTokens to every server, including myself iff me=True"
        
        for addr in self.serverAddresses:
            if addr == (self.ip, self.port) and me == False:
                continue
            self.sendMessage(msgTokens, addr)

    def handleIncomingMessages(self):
        cls = self.__class__

        while True:
            # Blocks until a message arrives
            data, addr = self.sock.recvfrom(4096)

            if addr[1] in self.brokenLinks:
                # For simulating broken links, the message never arrives
                continue

            msg = data.decode()
            if cls.debugMode:
                print(f"Received message \"{msg}\" from machine at {addr}")

            # Assuming message format is msgType-arg0-arg1-...-argK,
            msgArgs = msg.split('-')
            msgType = msgArgs[0]

            # From server
            if addr in self.serverAddresses:

                # TODO RequestVote RPC

                # TODO AppendEntries RPC

                # Receive "I am leader" from a server
                elif msg == "I am leader":
                    self.printLog(f"Received \"I am leader\" from {addr[1]}. Setting my leader hint and relinquishing my leader status")
                    self.leaderHintAddress = addr
                    self.isLeader = False

            # From client
            else:
                if msgType == "leader":
                    self.printLog(f"Nominated to be leader by client at {addr[1]}")
                    self.nominatorAddress = addr    # Track the nominator for responding
                    threading.Thread(target=self.electionPhase, daemon=True).start()

            # From either client or server

            # Receiving a request (possibly forwarded from another server)
            if msgType == "request":    # request-Operation-requestID
                op = eval(msgArgs[1])
                requestID = eval(msgArgs[2])
                request = (op, requestID)
                self.printLog(f"Received request {requestID} from {addr[1]}")

                if self.isLeader:
                    self.requestQueue.put(request)
                else:
                    # Forward request to leader hint
                    self.printLog(f"Forwarding request {requestID} to server at {self.leaderHintAddress[1]}")
                    self.sendMessage(msgArgs, self.leaderHintAddress)

    def _getAnswer(self, operation):
        "Returns the answer of performing operation on kvstore"
        if operation.type == "get":
            result = self.kvstore.get(operation.key)
            return result if result else "KEY_DOES_NOT_EXIST"

        elif operation.type == "put":
            return "success"

        else:
            return None

    def printLog(self, string):
        "Prints the input string with the server ID prefixed"
        print(f"[SERVER {self.ID}] {string}")


def handleUserInput():
    while True:
        cmdArgs = input().split()

        cmd = cmdArgs[0]

        if len(cmdArgs) == 1:
            if cmd == "failProcess":    # failProcess
                server.printLog("Crashing...")
                server.cleanExit()
                sys.exit()

            elif cmd == "debug":
                DEBUG()

        elif len(cmdArgs) == 2:
            if cmd == "failLink":   # failLink <destinationPort>
                dstPort = int(cmdArgs[1])
                if dstPort not in server.brokenLinks:
                    server.brokenLinks.add(dstPort)
                    server.printLog(f"Broke the link to {dstPort}")
                else:
                    server.printLog(f"The link to {dstPort} is already broken")

            elif cmd == "fixLink":  # fixLink <destinationPort>
                dstPort = int(cmdArgs[1])
                if dstPort in server.brokenLinks:
                    server.brokenLinks.remove(dstPort)
                    server.printLog(f"Fixed the link to {dstPort}")
                else:
                    server.printLog(f"The link to {dstPort} is not broken")

            elif cmd == "broadcast":    # broadcast <msg>
                msg = cmdArgs[1]
                server.broadcastToServers(msg)

            elif cmd == "print":
                varName = cmdArgs[1]
                print(f"{varName}:")

                if varName == "brokenLinks" or varName == "bl":
                    pprint.pprint(server.brokenLinks)

                elif varName == "blockchain" or varName == "bc":
                    pprint.pprint(server.blockchain._list)

                elif varName == "depth":
                    print(server.blockchain.depth)

                elif varName == "kvstore" or varName == "kv":
                    pprint.pprint(server.kvstore._dict)

                elif varName == "requestQueue" or varName == "rq":
                    pprint.pprint(server.requestQueue.queue)

                elif varName == "serverList" or varName == "sl":
                    print(server.serverAddresses)

                else:
                    print("Does not exist")

        elif len(cmdArgs) == 3:
            if cmd == "send":    # send <msg> <port>
                msg = cmdArgs[1]
                recipient = (socket.gethostname(), int(cmdArgs[2]))

                server.sendMessage((msg,), recipient)


def DEBUG():
    print(f"num of running threads: {threading.active_count()}")
    print(f"promiseCount: {server.promiseCount}")


# Parse cmdline args
if len(sys.argv) != 2:
    print(f"Usage: python3 {sys.argv[0]} serverID")
    sys.exit()

serverID = int(sys.argv[1])

server = Server(serverID)   # Start the server
server.start()

# Handle stdin
handleUserInput()
