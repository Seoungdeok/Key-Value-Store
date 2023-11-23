/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 *              Definition of MP1Node class functions. (Revised 2020)
 *
 *  Starter code template
 **********************************/

#include "MP1Node.h"
#include <random>


/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Params *params, EmulNet *emul, Log *log, Address *address) {
  for (int i = 0; i < 6; i++) {
    NULLADDR[i] = 0;
  }
  this->memberNode = new Member;
  this->shouldDeleteMember = true;
  memberNode->inited = false;
  this->emulNet = emul;
  this->log = log;
  this->par = params;
  this->memberNode->addr = *address;
}

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log,
                 Address *address) {
  for (int i = 0; i < 6; i++) {
    NULLADDR[i] = 0;
  }
  this->memberNode = member;
  this->shouldDeleteMember = false;
  this->emulNet = emul;
  this->log = log;
  this->par = params;
  this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {
  if (shouldDeleteMember) {
    delete this->memberNode;
  }
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into
 * the queue This function is called by a node to receive messages currently
 * waiting for it
 */
int MP1Node::recvLoop() {
  if (memberNode->bFailed) {
    return false;
  } else {
    return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1,
                           &(memberNode->mp1q));
  }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
  Queue q;
  return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 *                 All initializations routines for a member.
 *                 Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
  Address joinaddr;
  joinaddr = getJoinAddress();
  // Self booting routines
  if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
    exit(1);
  }

  if (!introduceSelfToGroup(&joinaddr)) {
    finishUpThisNode();
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
    exit(1);
  }

  return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
  /*
   * This function is partially implemented and may require changes
   */
  int id = *(int *)(&memberNode->addr.addr);
  int port = *(short *)(&memberNode->addr.addr[4]);

  memberNode->bFailed = false;
  memberNode->inited = true;
  memberNode->inGroup = false;
  // node is up!
  memberNode->nnb = 0;
  memberNode->heartbeat = 0;
  memberNode->pingCounter = TFAIL;
  memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);

  return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
  MessageHdr *msg;
#ifdef DEBUGLOG
  static char s[1024];
#endif

  if (0 ==
      strcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr))) {
    // I am the group booter (first process to join the group). Boot up the
    // group
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Starting up group...");
#endif
    memberNode->inGroup = true;
  } else {
    size_t msgsize =
        sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
    msg = (MessageHdr *)malloc(msgsize * sizeof(char));

    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREQ;
    memcpy((char *)(msg + 1), &memberNode->addr.addr,
           sizeof(memberNode->addr.addr));
    memcpy((char *)(msg + 1) + 1 + sizeof(memberNode->addr.addr),
           &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
    sprintf(s, "Trying to join...");
    log->LOG(&memberNode->addr, s);
#endif

    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

    free(msg);
  }

  return 1;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
  /*
   * Your code goes here
   */
  memberNode->inited = false;
  memberNode->memberList.clear();
  memberNode->inGroup = false;

  return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 *                 Check your messages in queue and perform membership protocol
 * duties
 */
void MP1Node::nodeLoop() {
  if (memberNode->bFailed) {
    return;
  }

  // Check my messages
  checkMessages();

  // Wait until you're in the group...
  if (!memberNode->inGroup) {
    return;
  }

  // ...then jump in and share your responsibilites!
  nodeLoopOps();

  return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message
 * handler
 */
void MP1Node::checkMessages() {
  void *ptr;
  int size;

  // Pop waiting messages from memberNode's mp1q
  while (!memberNode->mp1q.empty()) {
    ptr = memberNode->mp1q.front().elt;
    size = memberNode->mp1q.front().size;
    memberNode->mp1q.pop();
    recvCallBack((void *)memberNode, (char *)ptr, size);
  }
  return;
}

bool operator<(const Address &left, const Address &right) {
    return memcmp(left.addr, right.addr, sizeof(left.addr)) < 0;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
  MessageHdr *msg = (MessageHdr *)data;

  if (msg->msgType == JOINREQ) {
    // memberNode = cordinator
    // Extract the address of the sender and its current heartbeat:
    Address *addr = (Address *)(msg + 1);
    long *heartbeat =
        (long *)((char *)(msg + 1) + sizeof(memberNode->addr.addr));

    int id = addr->addr[0];
    short port = addr->addr[4];
    if (!isNodeInMemberList(id, port)) {
        // Add to member list
        MemberListEntry newEntry(id, port, *heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(newEntry);
    #ifdef DEBUGLOG
        //log->LOG(&memberNode->addr, "Added a new member after receiving JOINREQ.");
        //log->logNodeAdd(&memberNode->addr, addr);
    #endif
        // Create JOINREP message
        // Calculate Total Message Size
        size_t totalMembers = memberNode->memberList.size() + 1;
        size_t msgSize =
            sizeof(MessageHdr) + totalMembers * sizeof(MemberListEntry);
        //  Allocate Memory for JOINREP
        MessageHdr *repMsg = (MessageHdr *)malloc(msgSize * sizeof(char));

        repMsg->msgType = JOINREP;
        // A pointer (repData) is initialized to the data portion of the message,
        // immediately following the header.
        char *repData = (char *)(repMsg + 1);

        // Serialize self
        MemberListEntry self(*(int *)(&memberNode->addr.addr),
                            *(short *)(&memberNode->addr.addr[4]),
                            memberNode->heartbeat, par->getcurrtime());
        memcpy(repData, &self, sizeof(MemberListEntry));
        repData += sizeof(MemberListEntry);

        // Serialize member list
        for (const auto &member : memberNode->memberList) {
        memcpy(repData, &member, sizeof(MemberListEntry));
        repData += sizeof(MemberListEntry);
        }

        // Send JOINREP message to the new member
        emulNet->ENsend(&memberNode->addr, addr, (char *)repMsg, msgSize);

        free(repMsg);
    }
  } else if (msg->msgType == JOINREP) {
    data += sizeof(
        MessageHdr); // Skip the message header to point to the members list.
    int dataSizeLeft = size - sizeof(MessageHdr);

    // log->LOG(&memberNode->addr,  "dataSizeLeft %d", dataSizeLeft);
    while (dataSizeLeft > 0) {
      // Directly extract MemberListEntry details from the data.
      int memberId = *(int *)data;
      short memberPort = *(short *)(data + sizeof(int));
      long memberHeartbeat = *(long *)(data + sizeof(int) + sizeof(short) + 2);
      long memberTimestamp =
          *(long *)(data + sizeof(int) + sizeof(short) + sizeof(long) + 2);

      MemberListEntry receivedMember(memberId, memberPort, memberHeartbeat,
                                     memberTimestamp);

      // Check if the entry is not self.
      int selfId = *(int *)(&memberNode->addr.addr);
      if (receivedMember.getid() != selfId && !isNodeInMemberList(memberId, memberPort)) {
        // Add received MemberListEntry to the local member list.
        memberNode->memberList.push_back(receivedMember);

        // Create an Address object for the new member for logging.
        Address newMemberAddr;
        memcpy(&newMemberAddr.addr, &memberId, sizeof(int));
        memcpy(&newMemberAddr.addr[4], &memberPort, sizeof(short));

        // Log the addition of the new node.
        //log->LOG(&memberNode->addr, "Added a new member after receiving JOINREP");
        //log->logNodeAdd(&memberNode->addr, &newMemberAddr);
      }

      // Move the data pointer and decrease the size for the next iteration.
      data += sizeof(MemberListEntry);
      dataSizeLeft -= sizeof(MemberListEntry);
    }

    // Update the node's status as a member of the group.
    memberNode->inGroup = true;

    // Update the count of neighbors.
    memberNode->nnb = memberNode->memberList.size();

  } else if (msg->msgType == GOSSIP) {
    MemberListEntry *members = (MemberListEntry *)(msg + 1);
    int memberCount = (size - sizeof(MessageHdr)) / sizeof(MemberListEntry);

    // Update our own member list based on the gossip received
    for (int i = 0; i < memberCount; i++) {
      MemberListEntry &receivedMember = members[i];

        Address receivedAddr;
        int memberId = receivedMember.getid();
        short memberPort = receivedMember.getport();
        memcpy(&receivedAddr.addr, &memberId, sizeof(int));
        memcpy(&receivedAddr.addr[4], &memberPort, sizeof(short));

        if (failedNodes.find(receivedAddr) != failedNodes.end()) {
            //log->LOG(&memberNode->addr, "CONTINUED %d", memberId);
            continue;
        }

      // Check if member already exists in our list
      bool found = false;
      for (auto &member : memberNode->memberList) {
        if (member.getid() == receivedMember.getid() &&
            member.getport() == receivedMember.getport()) {
          found = true;
          // Update heartbeat if received heartbeat is greater
          if (receivedMember.getheartbeat() > member.getheartbeat()) {
            //log->LOG(&memberNode->addr, "Updated member %d.", member.getid());
            //log->LOG(&memberNode->addr, "BEFORE heartbeat: %d, timestamp: %d", member.getheartbeat(), member.gettimestamp());
            member.setheartbeat(receivedMember.getheartbeat());
            member.settimestamp(par->getcurrtime());
            //log->LOG(&memberNode->addr, "AFTER heartbeat: %d, timestamp: %d", member.getheartbeat(), member.gettimestamp());
          }
          break;
        }
      }
      if (!found && !isNodeInMemberList(memberId, memberPort)) {
        memberNode->memberList.push_back(receivedMember);

        // Log the addition of the new member
        Address newMemberAddr;
        memcpy(&newMemberAddr.addr, &memberId, sizeof(int));
        memcpy(&newMemberAddr.addr[4], &memberPort, sizeof(short));
        // Log the addition of the new node.
        //log->logNodeAdd(&memberNode->addr, &newMemberAddr);
      }
    }
// #ifdef DEBUGLOG
//     log->LOG(&memberNode->addr, "Updated member list after receiving GOSSIP.");
// #endif
  }
  return true;
}

/**
* FUNCTION NAME: nodeLoopOps
*
* DESCRIPTION: Check if any node hasn't responded within a timeout period and
then delete
*                 the nodes
*                 Propagate your membership list
*/
void MP1Node::nodeLoopOps() {
  // Increment heartbeat for this node
  memberNode->heartbeat++;

  // Update the heartbeat and timestamp for the current node in the membership
  // list
  for (auto &member : memberNode->memberList) {
    if (member.getid() == *(int *)(&memberNode->addr.addr) &&
        member.getport() == *(short *)(&memberNode->addr.addr[4])) {
      member.setheartbeat(memberNode->heartbeat);
      member.settimestamp(par->getcurrtime());
      break;
    }
  }

  // Gossip mechanism: share membership list with random nodes
  gossip();

    // Stale node removal and failure detection
    for (auto it = memberNode->memberList.begin();
        it != memberNode->memberList.end();) {
        int timeSinceLastUpdate = par->getcurrtime() - it->gettimestamp();

        Address currentAddr;
        int id = it->getid();
        short port = it->getport();
        memcpy(&currentAddr.addr, &id, sizeof(int));
        memcpy(&currentAddr.addr[4], &port, sizeof(short));

        if (timeSinceLastUpdate > TREMOVE) {
            // Remove stale node from the membership list
            it = memberNode->memberList.erase(it);
        } else if (timeSinceLastUpdate > TFAIL && failedNodes.find(currentAddr) == failedNodes.end()) {
            //log->logNodeRemove(&memberNode->addr, &currentAddr); // Log the node as failed/removed
            failedNodes.insert(currentAddr);  // Mark node as failed
            ++it;
        } else {
            ++it;
        }
    }

//   // Stale node removal and failure detection
//   for (auto it = memberNode->memberList.begin();
//        it != memberNode->memberList.end();) {
//     int timeSinceLastUpdate = par->getcurrtime() - it->gettimestamp();

//     if (timeSinceLastUpdate > TREMOVE) {
//       // Remove stale node from the membership list
//       Address staleAddr;
//       int id = it->getid();
//       short port = it->getport();
//       memcpy(&staleAddr.addr, &id, sizeof(int));
//       memcpy(&staleAddr.addr[4], &port, sizeof(short));

//         log->LOG(&memberNode->addr,
//                  "it->gettimestamp(): %d, it->getid(): %d, it->getheartbeat(): %d, timeSinceLastUpdate: %d",
//                  it->gettimestamp(), it->getid(), it->getheartbeat(), timeSinceLastUpdate);
//       log->logNodeRemove(&memberNode->addr, &staleAddr);
//       it = memberNode->memberList.erase(it);
//     } else {
//       ++it;
//     }
//   }
}

void MP1Node::gossip() {
  size_t listSize = memberNode->memberList.size();
  if (listSize == 0)
    return; // No nodes to gossip with

  // Prepare the gossip message
  size_t msgSize = sizeof(MessageHdr) + (listSize * sizeof(MemberListEntry));
  MessageHdr *gossipMsg = (MessageHdr *)malloc(msgSize);
  gossipMsg->msgType = GOSSIP;

  // Serialize the membership list into the gossip message
  MemberListEntry *serializedList = (MemberListEntry *)(gossipMsg + 1);
  for (size_t i = 0; i < listSize; i++) {
    serializedList[i] = memberNode->memberList[i];
  }

  for(int i = 0; i < memberNode->memberList.size(); i++) {
    // Randomly select a node to gossip with
    int randomIdx = rand() % listSize;
    Address destAddr;
    int id = memberNode->memberList[randomIdx].getid();
    short port = memberNode->memberList[randomIdx].getport();
    memcpy(&destAddr.addr, &id, sizeof(int));
    memcpy(&destAddr.addr[4], &port, sizeof(short));

    // Send gossip message
    emulNet->ENsend(&memberNode->addr, &destAddr, (char *)gossipMsg, msgSize);
  }
  free(gossipMsg);
}

bool MP1Node::isNodeInMemberList(int id, short port) {
    for (auto &member : memberNode->memberList) {
        if (member.getid() == id && member.getport() == port) {
            return true;
        }
    }
    return false;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
  return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
  Address joinaddr;

  *(int *)(&joinaddr.addr) = 1;
  *(short *)(&joinaddr.addr[4]) = 0;

  return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
  memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
  printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
         addr->addr[3], *(short *)&addr->addr[4]);
}
