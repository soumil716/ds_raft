# from tarfile import _Bz2ReadableFileobj
from math import fabs
import time
import socket
import json
import os
import socket
import time
import threading
import json
import traceback
from threading import Timer

class Message:
    def __init__(self) -> None:
        self.msg={}
    def setMsg(self,msg):
        self.msg=msg
    def getMsg(self):
        return self.setMsg
class Server:
    def __init__(self,UDP_Socket,) -> None:
        
        self.UDP_Socket=UDP_Socket
        self.currentTerm=0
        self.votedFor=""
        self.log=[]
        self.timeout=int(os.environ['timeout'])
        self.heartbeat=100
        self.currentState="follower"
        self.candidateId=os.environ['sender']
        self.vote=0
        self.voted=False
        self.sentVoteRequest=False
        self.leaderElected=False

    def listener(self):
        print(f"Starting Listener ")
        # while True:
        start_time=time.time()
        decoded_msg={}
        while time.time()<(start_time+self.timeout):
            try:
                msg, addr = self.UDP_Socket.recvfrom(1024)
                start_time=time.time()
            except:
                print(f"ERROR while fetching from socket : {traceback.print_exc()}")
            decoded_msg = json.loads(msg.decode('utf-8'))
            print("listner ",decoded_msg)
            self.process_msgs(decoded_msg)
            
        if len(msg)==0:
            start_time=time.time()
            self.process_msgs({})

        print("Exiting Listener Function")

    # Heartbeats
    def appendRPC(self,msg):

        if self.currentState=="leader":
            while True:
                msg1 = {"msg":os.environ['sender'],"heartbeat":self.heartbeat,"timeout":self.timeout}
                msg_bytes = json.dumps(msg1).encode()
                try:
                    self.UDP_Socket.sendto(msg_bytes, (os.environ['target1'], 5555))
                    self.UDP_Socket.sendto(msg_bytes, (os.environ['target2'], 5555))
                except:
                     print(f"ERROR while fetching from socket : {traceback.print_exc()}")
        elif self.currentState=="candidate" and ('Term' in msg):
            # print(msg)
            msg_bytes = json.dumps(msg).encode()
            try:
                self.UDP_Socket.sendto(msg_bytes, (os.environ['target1'], 5555))
                self.UDP_Socket.sendto(msg_bytes, (os.environ['target2'], 5555))
                print("Send Vote request to "+os.environ['target1']+" and "+os.environ['target2']+ " and", msg)
            except:
                 print(f"ERROR while fetching from socket : {traceback.print_exc()}")
        
        elif self.currentState=="candidate" and 'votedFor' in msg:
            msg_bytes = json.dumps(msg).encode()
            try:
                self.UDP_Socket.sendto(msg_bytes, (msg['votedFor'], 5555))
                print("send vote to "+ msg['votedFor'])
            except:
                 print(f"ERROR while fetching from socket : {traceback.print_exc()}")
            # print(self.currentState,self.vote)
        elif self.currentState=="candidate" and ('Elected_leader' in msg):
            # print(msg)
            msg_bytes = json.dumps(msg).encode()
            self.currentState="leader"
            try:
                self.UDP_Socket.sendto(msg_bytes, (os.environ['target1'], 5555))
                self.UDP_Socket.sendto(msg_bytes, (os.environ['target2'], 5555))
                print("elected leader and intimating to "+os.environ['target1']+" and "+os.environ['target2']+ " and the current state is"+self.currentState)
            except:
                 print(f"ERROR while fetching from socket : {traceback.print_exc()}")
            self.appendRPC({})

    def process_msgs(self,msg):
        print(msg)
        if len(msg)==0:
            print("changing state to candidate")
            self.currentTerm+=1
            self.currentState="candidate"
            self.vote+=1
            self.sentVoteRequest=True
            msg={"Term":self.currentTerm,"candidateId":self.candidateId,"lastLogIndex":0,"lastLogTerm":0}
            self.appendRPC(msg)
        
        # print(self.currentTerm)

        if not self.voted and ('Term'in msg and msg['Term']>self.currentTerm-1) and self.currentState!="leader" and self.candidateId!=msg['candidateId']:
            print("voting phase",msg)
            self.votedFor=msg['candidateId']
            msg={"votedFor":self.votedFor}
            self.voted=True
            self.appendRPC(msg)

        if 'votedFor' in msg and msg['votedFor']==self.candidateId and not self.leaderElected:
            print("counting votes")
            self.vote+=1
            if self.vote>2:
                print("if in counting votes " ,self.vote)
                # self.currentState="leader"
                self.timeout=int(os.environ['timeout'])
                self.leaderElected=True
                msg={'Elected_leader':self.candidateId,'votes':self.vote}
                # self.voted=False
                self.appendRPC(msg)
            else:
                print("else in counting votes")
                self.appendRPC({})
        if 'Elected_leader' in msg:
            print("elected node")
            self.currentState="follower"
            self.leaderElected=True
            self.voted=False
        if 'sender_name' in msg and msg['sender_name']=='Controller':
            print('Inside Controller')
            self.currentState="follower"
            self.appendRPC({})

if __name__ == "__main__":
    print("Starting"+os.environ['sender'])
    # time.sleep(5)


    # Creating Socket and binding it to the target container IP and port
    UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    # Bind the node to sender ip and port
    UDP_Socket.bind((os.environ['sender'], 5555))

    # Sending 5 messages to Node 2
    # for i in range(5):
    server = Server(UDP_Socket)
    #Starting thread 1
    
    threading.Thread(target=server.listener).start()
    
    #Starting thread 2
    # threading.Thread(target=server.send_msg,args=[0]).start()
    # server.send_msg(0)

    #Starting thread 3
    # time.sleep(5)
    threading.Thread(target=server.appendRPC,args=[{}]).start()

    threading.Thread(target=server.process_msgs,args=[{}]).start()
    # server.appendRPC(0)

    # print("Started all functions, Sleeping on the main thread for 10 seconds now")
    # time.sleep(10)
    print(f"Completed Node Main Thread"+os.environ['sender'])
    
