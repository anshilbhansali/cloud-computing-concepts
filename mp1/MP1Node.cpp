/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 * IMPLEMENTATION OF GOSSIP FAILURE DETECTION PROTOCOL
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
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
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
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
	//int id = *(int*)(&memberNode->addr.addr);
	//int port = *(short*)(&memberNode->addr.addr[4]);

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

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        memberNode->memberList.push_back(MemberListEntry(
            int(memberNode->addr.addr[0]),
            short(memberNode->addr.addr[4]),
            memberNode->heartbeat,
            this->par->getcurrtime()
            )
        );

        Address newAddr;
        newAddr.init();
        newAddr.addr[0] = int(memberNode->addr.addr[0]);
        newAddr.addr[4] = short(memberNode->addr.addr[4]);
        log->logNodeAdd(&memberNode->addr, &newAddr);
    }
    else {
        // i am not the introducer, so send JOINREQ msg to introducer
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
        cout<<s<<": "<<memberNode->addr.getAddress()<<endl;
        if (msg->msgType == JOINREQ){
            cout<<memberNode->addr.getAddress()<<" is sending msg JOINREQ to "<<joinaddr->getAddress()<<endl;
        }
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
int MP1Node::finishUpThisNode(){
    memberNode->bFailed = false;
    memberNode->inited = false;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    memberNode->memberList.clear();

    return 0;
}

void MP1Node::incrHeartBeat(){
    memberNode->heartbeat++;
    for (auto &entry : memberNode->memberList) {
        if((entry.id) == int(memberNode->addr.addr[0])) {
            entry.setheartbeat(memberNode->heartbeat);
            entry.settimestamp(this->par->getcurrtime());
            break;
        }
    }
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    incrHeartBeat();
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    removeFailedNodes();
    sendGossipMsg();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    MessageHdr *recvMsg;
    Address sourceAddress;
    long heartbeat;

    recvMsg = (MessageHdr *) data;
    memcpy(&sourceAddress, data+sizeof(MessageHdr), sizeof(memberNode->addr.addr));
    memcpy(&heartbeat, data+sizeof(MessageHdr)+sizeof(memberNode->addr.addr)+1, sizeof(long));

    if (recvMsg->msgType == JOINREQ) {
        // random node has sent me (introducer) the JOINREQ msg
        cout<<"\n"<<memberNode->addr.getAddress()<<" received JOINREQ msg from "<<sourceAddress.getAddress()<<endl;
        cout<<"with heartbeat: "<<heartbeat<<endl;
        cout<<"memberlist size: "<<memberNode->memberList.size()<<endl;

        // add to my local list
        cout<<"adding to introducers list: "<<int(sourceAddress.addr[0])<<endl;
        memberNode->memberList.push_back(MemberListEntry(
            int(sourceAddress.addr[0]),
            short(sourceAddress.addr[4]),
            heartbeat,
            this->par->getcurrtime()
            )
        );

        // log that I added sourceAddress
        log->logNodeAdd(&memberNode->addr, &sourceAddress);

        // Send JOINREP msg with memberlist in it
        // need to specify memberlist in th JOINREP msg
        int listsize = memberNode->memberList.size();

        // msg will be of structure: MessageHdr Address hearbeat listsize [memberlistentry]
        MessageHdr *msgToSend;

        // id + port + heartbeat + timestamp
        size_t memberlistentrysize = sizeof(int)+sizeof(short)+sizeof(long)+sizeof(long);
        size_t msgsize = sizeof(MessageHdr)+(sizeof(memberNode->addr.addr)+1)+sizeof(long)+sizeof(int)+listsize*memberlistentrysize;
        cout<<"msgsize: "<<msgsize<<" bytes"<<endl;

        msgToSend = (MessageHdr*) malloc(msgsize*sizeof(char));
        msgToSend->msgType = JOINREP;

        int offset = 0;

        memcpy((char *)(msgToSend+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        offset += sizeof(memberNode->addr.addr)+1;

        memcpy((char *)(msgToSend+1)+offset, &memberNode->heartbeat, sizeof(long));
        offset += sizeof(long);

        memcpy((char *)(msgToSend+1)+offset, &listsize, sizeof(int));
        offset+=sizeof(int);

        // package my memberlist in msg
        for (int i=0; i<listsize; i++){
            memcpy((char *)(msgToSend+1)+offset, &memberNode->memberList[i].id, sizeof(int));
            offset+=sizeof(int);

            memcpy((char *)(msgToSend+1)+offset, &memberNode->memberList[i].port, sizeof(short));
            offset+=sizeof(short);

            memcpy((char *)(msgToSend+1)+offset, &memberNode->memberList[i].heartbeat, sizeof(long));
            offset+=sizeof(long);

            memcpy((char *)(msgToSend+1)+offset, &memberNode->memberList[i].timestamp, sizeof(long));
            offset+=sizeof(long);
        }

        // send reply msg to sourceAddress
        emulNet->ENsend(&memberNode->addr, &sourceAddress, (char *)msgToSend, msgsize);
        free(msgToSend);

    } else if (recvMsg->msgType == JOINREP){
        // introducer node has replied to me with JOINREP and its memberlist
        cout<<"\n"<<memberNode->addr.getAddress()<<" received JOINREP msg from "<<sourceAddress.getAddress()<<endl;
        cout<<"with heartbeat: "<<heartbeat<<endl;

        // add myself to the group and combine received member list with my local list
        memberNode->inGroup = true;
        memberNode->inited = true;

        // add myself to my local memberlist
        MemberListEntry mle = MemberListEntry(
            int(memberNode->addr.addr[0]),
            short(memberNode->addr.addr[4]),
            memberNode->heartbeat,
            this->par->getcurrtime()
        );
        memberNode->memberList.push_back(mle);

        // UNPACKAGE introducer's memberlist into introducerMemberList
        int recvlistsize;
        memcpy(&recvlistsize, data+sizeof(MessageHdr)+sizeof(memberNode->addr.addr)+1+sizeof(long), sizeof(int));
        cout<<"with recvlistsize: "<<recvlistsize<<endl;
        cout<<"UNPACK introducers remote list"<<endl;
        vector<MemberListEntry> introducerMemberList;
        int offset = sizeof(MessageHdr)+sizeof(memberNode->addr.addr)+1+sizeof(long)+sizeof(int);

        int memberID;
        short memberPort;
        long memberHeartbeat, memberTimestamp;
        for (int i=0; i<recvlistsize; i++){
            memcpy(&memberID, data+offset, sizeof(int));
            offset+=sizeof(int);
            memcpy(&memberPort, data+offset, sizeof(short));
            offset+=sizeof(short);
            memcpy(&memberHeartbeat, data+offset, sizeof(long));
            offset+=sizeof(long);
            memcpy(&memberTimestamp, data+offset, sizeof(long));
            offset+=sizeof(long);

            introducerMemberList.push_back(MemberListEntry(memberID, memberPort, memberHeartbeat, memberTimestamp));
        }

        // MERGE introducerMemberList into my local memberlist
        cout<<"introducerMemberList: "<<introducerMemberList.size()<<endl;
        cout<<"memberNode->memberList before MERGE: "<<memberNode->memberList.size()<<endl;

        bool foundNewMember;
        for (auto &remoteMember : introducerMemberList){
            foundNewMember = true; // current iteration remoteMember is new

            for (auto &localMember : memberNode->memberList){
                if (localMember.id == remoteMember.id){
                    foundNewMember = false; // its found in local list, so its not new
                    if (remoteMember.heartbeat > localMember.heartbeat){
                        localMember.setheartbeat(remoteMember.heartbeat);
                        localMember.settimestamp(this->par->getcurrtime());
                    }
                    break;
                }
            }

            // if a new member is found in introducerMemberList AND its last timestamp is not too far away
            if (foundNewMember && remoteMember.gettimestamp()>=(this->par->getcurrtime() - TFAIL)){
                // new member is a live noded, so add to my list
                memberNode->memberList.push_back(MemberListEntry(remoteMember));

                // log that I added a new member to my list
                Address newAddr;
                newAddr.init();
                newAddr.addr[0] = remoteMember.id;
                newAddr.addr[4] = remoteMember.port;
                log->logNodeAdd(&memberNode->addr,&newAddr);
            }
        }

        // update number of neighbours
        memberNode->nnb = memberNode->memberList.size() - 1;

        cout<<"memberNode->memberList after MERGING: "<<memberNode->memberList.size()<<endl;

    } else if (recvMsg->msgType == GOSSIP){
        // any node can receive a gossip msg
        cout<<"\n"<<memberNode->addr.getAddress()<<" received GOSSIP msg from "<<sourceAddress.getAddress()<<endl;
        cout<<"with heartbeat: "<<heartbeat<<endl;

        // UNPACKAGE memberlist received in msg
        int recvlistsize;
        memcpy(&recvlistsize, data+sizeof(MessageHdr)+sizeof(memberNode->addr.addr)+1+sizeof(long), sizeof(int));
        cout<<"with gossiprecvlistsize: "<<recvlistsize<<endl;
        vector<MemberListEntry> remoteMemberList;
        int offset = sizeof(MessageHdr)+sizeof(memberNode->addr.addr)+1+sizeof(long)+sizeof(int);

        int memberID;
        short memberPort;
        long memberHeartbeat, memberTimestamp;
        for (int i=0; i<recvlistsize; i++){
            memcpy(&memberID, data+offset, sizeof(int));
            offset+=sizeof(int);
            memcpy(&memberPort, data+offset, sizeof(short));
            offset+=sizeof(short);
            memcpy(&memberHeartbeat, data+offset, sizeof(long));
            offset+=sizeof(long);
            memcpy(&memberTimestamp, data+offset, sizeof(long));
            offset+=sizeof(long);

            remoteMemberList.push_back(MemberListEntry(memberID, memberPort, memberHeartbeat, memberTimestamp));
        }

        // MERGE remoteMemberList into my local memberlist
        cout<<"remoteMemberList: "<<remoteMemberList.size()<<endl;
        cout<<"memberNode->memberList before MERGE: "<<memberNode->memberList.size()<<endl;

        bool foundNewMember;
        for (auto &remoteMember : remoteMemberList){
            foundNewMember = true; // current iteration remoteMember is new

            for (auto &localMember : memberNode->memberList){
                if (localMember.id == remoteMember.id){
                    foundNewMember = false; // its found in local list, so its not new
                    if (remoteMember.heartbeat > localMember.heartbeat){
                        localMember.setheartbeat(remoteMember.heartbeat);
                        localMember.settimestamp(this->par->getcurrtime());
                    }
                    break;
                }
            }

            // if a new member is found in remoteMemberList AND its last timestamp is not too far away
            if (foundNewMember && remoteMember.gettimestamp()>=(this->par->getcurrtime() - TFAIL)){
                // new member is a live noded, so add to my list
                memberNode->memberList.push_back(MemberListEntry(remoteMember));

                // log that I added a new member to my list
                Address newAddr;
                newAddr.init();
                newAddr.addr[0] = remoteMember.id;
                newAddr.addr[4] = remoteMember.port;
                log->logNodeAdd(&memberNode->addr,&newAddr);
            }
        }
        // update number of neighbours
        memberNode->nnb = memberNode->memberList.size() - 1;

        cout<<"memberNode->memberList after MERGING: "<<memberNode->memberList.size()<<endl;


    } else {
        cout<<"what message is this?: "<<recvMsg->msgType<<endl;
    }
}

void MP1Node::sendGossipMsg() {
    // SEND GOSSIP message to all members in my list
    // package memberlist in msg
    MessageHdr *msg;

    int listsize = memberNode->memberList.size();

    // id + port + heartbeat + timestamp
    size_t memberlistentrysize = sizeof(int)+sizeof(short)+sizeof(long)+sizeof(long);
    size_t msgsize = sizeof(MessageHdr)+(sizeof(memberNode->addr.addr)+1)+sizeof(long)+sizeof(int)+listsize*memberlistentrysize;

    msg = (MessageHdr*) malloc(msgsize*sizeof(char));
    msg->msgType = GOSSIP;

    int offset = 0;

    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    offset += sizeof(memberNode->addr.addr)+1;

    memcpy((char *)(msg+1)+offset, &memberNode->heartbeat, sizeof(long));
    offset += sizeof(long);

    memcpy((char *)(msg+1)+offset, &listsize, sizeof(int));
    offset+=sizeof(int);

    // package my memberlist in msg
    for (int i=0; i<listsize; i++){
        memcpy((char *)(msg+1)+offset, &memberNode->memberList[i].id, sizeof(int));
        offset+=sizeof(int);

        memcpy((char *)(msg+1)+offset, &memberNode->memberList[i].port, sizeof(short));
        offset+=sizeof(short);

        memcpy((char *)(msg+1)+offset, &memberNode->memberList[i].heartbeat, sizeof(long));
        offset+=sizeof(long);

        memcpy((char *)(msg+1)+offset, &memberNode->memberList[i].timestamp, sizeof(long));
        offset+=sizeof(long);
    }

    // send my list to all members in my list
    // do we need to send to all? maybe random subset?
    Address toAddress;
    for (auto &localMember : memberNode->memberList){
        if (int(memberNode->addr.addr[0])==int(localMember.id)){
            // skip myself
            continue;
        }
        if (localMember.gettimestamp() < this->par->getcurrtime() - TFAIL){
            // skip dead nodes
            continue;
        }
        toAddress.init();
        toAddress.addr[0] = int(localMember.id);
        toAddress.addr[4] = short(localMember.port);
        cout<<"SENDING GOSSIP MSG FROM "<<memberNode->addr.getAddress()<<" TO "<<toAddress.getAddress()<<endl;
        emulNet->ENsend(&memberNode->addr, &toAddress, (char *)msg, msgsize);
    }

    free(msg);
}

void MP1Node::removeFailedNodes() {
    vector<MemberListEntry>::iterator it;
    MemberListEntry mle;
    for (it = memberNode->memberList.begin(); it < memberNode->memberList.end(); it++) {
        mle = *it;
        if (int(mle.id) == int(memberNode->addr.addr[0])){
            continue;
        }
        if (mle.gettimestamp() < this->par->getcurrtime() - TREMOVE) {
            memberNode->memberList.erase(it);
            Address removedAddr;
            removedAddr.init();
            removedAddr.addr[0] = mle.id;
            removedAddr.addr[4] = mle.port;
            log->logNodeRemove(&memberNode->addr,&removedAddr);
        }
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    return;
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

    memset(&joinaddr, 0, sizeof(Address));
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
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
