/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition (Revised 2020)
 *
 * MP2 Starter template version
 **********************************/
#include "MP2Node.h"
#include "common.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log,
                 Address *address) {
  this->memberNode = memberNode;
  this->par = par;
  this->emulNet = emulNet;
  this->log = log;
  ht = new HashTable();
  this->memberNode->addr = *address;
  this->delimiter = "::";
}

/**
 * Destructor
 */
MP2Node::~MP2Node() { delete ht; }

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 *                 1) Gets the current membership list from the Membership
 * Protocol (MP1Node) The membership list is returned as a vector of Nodes. See
 * Node class in Node.h 2) Constructs the ring based on the membership list 3)
 * Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
  /*
   * Implement this. Parts of it are already implemented
   */
  vector<Node> curMemList;
  bool change = false;

  /*
   *  Step 1. Get the current membership list from Membership Protocol / MP1
   */
  curMemList = getMembershipList();

  /*
   * Step 2: Construct the ring
   */
  // Sort the list based on the hashCode
  sort(curMemList.begin(), curMemList.end());

  /*
   * Step 3: Run the stabilization protocol IF REQUIRED
   */
  // Check if there's a change in the ring
  // Determine if there's a change in the membership list
    change = (curMemList.size() != ring.size());
    if (change) {
      log->LOG(&memberNode->addr, "CHANGE FOUND: DIFFERENT SIZE");
    }
    if (!change) {
      for (size_t i = 0; i < ring.size(); i++) {
        if (ring[i].getHashCode() != curMemList[i].getHashCode()) {
          log->LOG(&memberNode->addr,
                   "CHANGE FOUND: SAME SIZE BUT DIFFERENT HASH CODE");
          change = true;
          break;
        }
      }
    }

    // Update the ring and run the stabilization protocol if there's a change
    if (change) {
      ring = curMemList;
      stabilizationProtocol();
    }
}

/**
 * FUNCTION NAME: getMembershipList
 *
 * DESCRIPTION: This function goes through the membership list from the
 * Membership protocol/MP1 and i) generates the hash code for each member ii)
 * populates the ring member in MP2Node class It returns a vector of Nodes. Each
 * element in the vector contain the following fields: a) Address of the node b)
 * Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
  unsigned int i;
  vector<Node> curMemList;
  for (i = 0; i < this->memberNode->memberList.size(); i++) {
    Address addressOfThisMember;
    int id = this->memberNode->memberList.at(i).getid();
    short port = this->memberNode->memberList.at(i).getport();
    memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
    memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
    curMemList.emplace_back(Node(addressOfThisMember));
  }
  return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the
 * ring HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
  std::hash<string> hashFunc;
  size_t ret = hashFunc(key);
  return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 *                 The function does the following:
 *                 1) Constructs the message
 *                 2) Finds the replicas of this key
 *                 3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
  // Increment the transaction ID first
  int currentTransID = g_transID++;

  // Store transaction context for Create operation
  TransactionInfo info;
  info.replyCount = 0;
  info.failCount = 0;
  info.opType = CREATE;
  info.key = key;
  info.value = value;
  info.time = par->getcurrtime();
  transactionStatus[currentTransID] = info;

  // Construct the message with the new transaction ID
  	 Message createMsg1(currentTransID, memberNode->addr, CREATE, key, value, PRIMARY);
	 Message createMsg2(currentTransID, memberNode->addr, CREATE, key, value, SECONDARY);
	 Message createMsg3(currentTransID, memberNode->addr, CREATE, key, value, TERTIARY);

  // Find the replicas of this key
  vector<Node> replicas = findNodes(key);

  // Send a message to each replica
  	 emulNet->ENsend(&memberNode->addr, replicas[0].getAddress(), createMsg1.toString());
	 emulNet->ENsend(&memberNode->addr, replicas[1].getAddress(), createMsg2.toString());
	 emulNet->ENsend(&memberNode->addr, replicas[2].getAddress(), createMsg3.toString());
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 *                 The function does the following:
 *                 1) Constructs the message
 *                 2) Finds the replicas of this key
 *                 3) Sends a message to the replica
 */
void MP2Node::clientRead(string key) {
  // Increment the transaction ID first
  int currentTransID = g_transID++;

  // Store transaction context for Read operation
  TransactionInfo readInfo;
  readInfo.replyCount = 0;
  readInfo.failCount = 0;
  readInfo.time = par->getcurrtime();
  readInfo.opType = READ;
  readInfo.key = key;
  transactionStatus[currentTransID] = readInfo;

  // Construct the message with the new transa sction ID
  Message readMsg(currentTransID, memberNode->addr, READ, key);

  // Find the replicas of this key
  vector<Node> replicas = findNodes(key);

  // Send a message to each replica
  for (Node replica : replicas) {
    emulNet->ENsend(&memberNode->addr, replica.getAddress(),
                    readMsg.toString());
  }
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 *                 The function does the following:
 *                 1) Constructs the message
 *                 2) Finds the replicas of this key
 *                 3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value) {
  // Increment the transaction ID first
  int currentTransID = g_transID++;

  // Store transaction context for Update operation
  TransactionInfo updateInfo;
  updateInfo.replyCount = 0;
  updateInfo.failCount = 0;
  updateInfo.time = par->getcurrtime();
  updateInfo.opType = UPDATE;
  updateInfo.key = key;
  updateInfo.value = value;
  transactionStatus[currentTransID] = updateInfo;

  // Construct the message with the new transaction ID
  Message updateMsg(currentTransID, memberNode->addr, UPDATE, key, value);

  // Find the replicas of this key
  vector<Node> replicas = findNodes(key);

  // Send a message to each replica
  for (Node replica : replicas) {
    emulNet->ENsend(&memberNode->addr, replica.getAddress(),
                    updateMsg.toString());
  }
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 *                 The function does the following:
 *                 1) Constructs the message
 *                 2) Finds the replicas of this key
 *                 3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key) {
  // Increment the transaction ID first
  int currentTransID = g_transID++;

  // Store transaction context for DELETE operation
  TransactionInfo info;
  info.replyCount = 0;
  info.failCount = 0;
  info.time = par->getcurrtime();
  info.opType = DELETE;
  info.key = key;
  info.value = ""; // value is not used in DELETE operation

  transactionStatus[currentTransID] = info;

  // Construct the DELETE message with the new transaction ID
  Message deleteMsg(currentTransID, memberNode->addr, DELETE, key);

  // Find the replicas of this key
  vector<Node> replicas = findNodes(key);

  // Send a message to each replica
  for (Node replica : replicas) {
    emulNet->ENsend(&memberNode->addr, replica.getAddress(),
                    deleteMsg.toString());
  }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 *                    The function does the following:
 *                    1) Inserts key value into the local hash table
 *                    2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
  	if (!ht->read(key).empty())
	{
		return false;
	}

  // Insert key and serialized entry value into the hash table
  bool success = ht->create(key, value);
  return success;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 *                 This function does the following:
 *                 1) Read key from local hash table
 *                 2) Return value
 */
string MP2Node::readKey(string key) {
  // Read key from local hash table
  string value = ht->read(key);

  // Determine success based on whether the value is found
  bool success = !value.empty();

  return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 *                 This function does the following:
 *                 1) Update the key to the new value in the local hash table
 *                 2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
  // Check if the key exists in the hash table
  bool keyExists = !ht->read(key).empty();

  // Update key in local hash table and return true or false
  bool success =
      keyExists &&
      ht->update(key,
                 Entry(value, par->getcurrtime(), replica).convertToString());

  return success;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 *                 This function does the following:
 *                 1) Delete the key from the local hash table
 *                 2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
  // Delete the key from the local hash table
  bool success = ht->deleteKey(key);

  return success;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 *                 This function does the following:
 *                 1) Pops messages from the queue
 *                 2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
    char *data;
    int size;

    // Loop through all messages in the queue
    while (!memberNode->mp2q.empty()) {
        // Pop a message from the queue
        data = (char *)memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();

        // Convert the message from char* to string for easier handling
        string message(data, data + size);

        // Handle the message types here
        Message msg(message);
        bool result;
        switch (msg.type) {
            case CREATE:
                result = createKeyValue(msg.key, msg.value, msg.replica);
                {
                    // Send reply back to the coordinator with the same transaction ID
                    Message replyMsg(msg.transID, memberNode->addr, REPLY, result);
                    emulNet->ENsend(&memberNode->addr, &msg.fromAddr, replyMsg.toString());
                }
                if (result) {
                    log->logCreateSuccess(&memberNode->addr, false, msg.transID, msg.key,
                                        msg.value);
                } else {
                    log->logCreateFail(&memberNode->addr, false, msg.transID, msg.key,
                                    msg.value);
                }
                break;
            case READ: 
                {
                    string value = readKey(msg.key);
                    result = !value.empty();

                    Message readReplyMsg(msg.transID, memberNode->addr, value);
                    emulNet->ENsend(&memberNode->addr, &msg.fromAddr,
                                    readReplyMsg.toString());
                    if (!value.empty()) {
                        log->logReadSuccess(&memberNode->addr, false, msg.transID, msg.key,
                                            value);
                    } else {
                        log->logReadFail(&memberNode->addr, false, msg.transID, msg.key);
                    }
                } 
                break;
            case UPDATE:
                result = updateKeyValue(msg.key, msg.value, msg.replica);
                {
                    // Send reply back to the coordinator with the same transaction ID
                    Message replyMsg(msg.transID, memberNode->addr, REPLY, result);
                    emulNet->ENsend(&memberNode->addr, &msg.fromAddr, replyMsg.toString());
                }
                if (result) {
                    log->logUpdateSuccess(&memberNode->addr, false, msg.transID, msg.key,
                                        msg.value);
                } else {
                    log->logUpdateFail(&memberNode->addr, false, msg.transID, msg.key,
                                    msg.value);
                }
                break;
            case DELETE:
                result = deletekey(msg.key);
                {
                    // Send reply back to the coordinator with the same transaction ID
                    Message replyMsg(msg.transID, memberNode->addr, REPLY, result);
                    emulNet->ENsend(&memberNode->addr, &msg.fromAddr, replyMsg.toString());
                }
                if (result) {
                    log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
                } else {
                    log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);
                }
                break;
            case REPLY: 
                {
                    TransactionInfo &txnInfo = transactionStatus[msg.transID];
                    if (txnInfo.isSuccess || txnInfo.isFailed)
                        continue;
                    if(msg.success) {
                            //if we have already one successful reply
                        if (txnInfo.replyCount > 0) {
                            switch (txnInfo.opType) {
                                case CREATE:
                                    log->logCreateSuccess(&memberNode->addr, true, msg.transID, txnInfo.key, txnInfo.value);
                                    break;
                                case UPDATE:
                                    log->logUpdateSuccess(&memberNode->addr, true, msg.transID, txnInfo.key, txnInfo.value);
                                    break;
                                case DELETE:
                                    log->logDeleteSuccess(&memberNode->addr, true, msg.transID, txnInfo.key);
                                    break;
                            }
                            txnInfo.isSuccess = true;
                        } else {
                            txnInfo.replyCount += 1;
                        }
                    } else {
                        if (txnInfo.failCount > 0) {
                            switch (txnInfo.opType) {
                                case CREATE:
                                    log->logCreateFail(&memberNode->addr, true, msg.transID, txnInfo.key, txnInfo.value);
                                    break;
                                case UPDATE:
                                    log->logUpdateFail(&memberNode->addr, true, msg.transID, txnInfo.key, txnInfo.value);
                                    break;
                                case DELETE:
                                    log->logDeleteFail(&memberNode->addr, true, msg.transID, txnInfo.key);
                                    break;
                            }
                            txnInfo.isFailed = true;
                        } else {
                            txnInfo.failCount += 1;
                        }
                    }
                    // After logging success or failure, clear the transaction
                    if (txnInfo.isSuccess || txnInfo.isFailed) {
                        transactionStatus.erase(msg.transID);
                    }
                }
                break;
            case READREPLY: {
                TransactionInfo &txnInfo = transactionStatus[msg.transID];
                if (txnInfo.isSuccess || txnInfo.isFailed)
                    continue;
                // Check if the value is non-empty (indicating success)
                if (!msg.value.empty()) {
                    if (txnInfo.replyCount > 0) {
                        log->logReadSuccess(&memberNode->addr, true, msg.transID, txnInfo.key, msg.value);
                        txnInfo.isSuccess = true;
                    } else {
                        txnInfo.replyCount += 1;
                    }
                } else {
                    if (txnInfo.failCount > 0) {
                        log->logReadFail(&memberNode->addr, true, msg.transID, txnInfo.key);
                        txnInfo.isFailed = true;
                    } else {
                        txnInfo.failCount += 1;
                    }
                }
                
                // After logging success or failure, clear the transaction
                if (txnInfo.isSuccess || txnInfo.isFailed) {
                    transactionStatus.erase(msg.transID);
                }
                break;
            }
        }
    }
    for (auto &txnPair : transactionStatus) {
        TransactionInfo &txnInfo = txnPair.second;
        int currentTime = par->getcurrtime();
        
        if (!txnInfo.isSuccess && !txnInfo.isFailed && (currentTime - txnInfo.time > TIMEOUT)) {
            switch (txnInfo.opType) {
                case CREATE:
                    log->logCreateFail(&memberNode->addr, true, txnPair.first, txnInfo.key, txnInfo.value);
                    break;
                case READ:
                    log->logReadFail(&memberNode->addr, true, txnPair.first, txnInfo.key);
                    break;
                case UPDATE:
                    log->logUpdateFail(&memberNode->addr, true, txnPair.first, txnInfo.key, txnInfo.value);
                    break;
                case DELETE:
                    log->logDeleteFail(&memberNode->addr, true, txnPair.first, txnInfo.key);
                    break;
            }
            txnInfo.isFailed = true;
        }
    }
}
/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 *                 This function is responsible for finding the replicas of a
 * key
 */
vector<Node> MP2Node::findNodes(string key) {
  size_t pos = hashFunction(key);
  vector<Node> addr_vec;
  if (ring.size() >= 3) {
    // if pos <= min || pos > max, the leader is the min
    if (pos <= ring.at(0).getHashCode() ||
        pos > ring.at(ring.size() - 1).getHashCode()) {
      addr_vec.emplace_back(ring.at(0));
      addr_vec.emplace_back(ring.at(1));
      addr_vec.emplace_back(ring.at(2));
    } else {
      // go through the ring until pos <= node
      for (int i = 1; i < (int)ring.size(); i++) {
        Node addr = ring.at(i);
        if (pos <= addr.getHashCode()) {
          addr_vec.emplace_back(addr);
          addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
          addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
          break;
        }
      }
    }
  }
  return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
  if (memberNode->bFailed) {
    return false;
  } else {
    return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1,
                           &(memberNode->mp2q));
  }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
  Queue q;
  return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and
 * leaves It ensures that there always 3 copies of all keys in the DHT at all
 * times The function does the following: 1) Ensures that there are three
 * "CORRECT" replicas of all the keys in spite of failures and joins Note:-
 * "CORRECT" replicas implies that every key is replicated in its two
 * neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
  for (auto &entry : ht->hashTable) {
    string key = entry.first;
    vector<Node> responsibleNodes = findNodes(key);

    // Handle re-replication for primary node
    if (responsibleNodes[0].nodeAddress == memberNode->addr) {
      for (int i = 1; i < responsibleNodes.size(); i++) {
        ReplicaType replicaType = (i == 1) ? SECONDARY : TERTIARY;
        Message msg(g_transID++, memberNode->addr, CREATE, key, entry.second,
                    replicaType);
        emulNet->ENsend(&memberNode->addr, &responsibleNodes[i].nodeAddress,
                        msg.toString());
      }
    }

    // Remove keys that are no longer the responsibility of this node
    if (std::find_if(responsibleNodes.begin(), responsibleNodes.end(),
                     [this](const Node &node) {
                       return node.nodeAddress == memberNode->addr;
                     }) == responsibleNodes.end()) {
      ht->deleteKey(key);
    }
  }
}
