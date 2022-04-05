# from tarfile import _Bz2ReadableFileobj

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

class Server:
    def __init__(self,UDP_Socket) -> None:
        
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

    # def create_msg(self,counter):
    #     msg = {"msg": f"Hi, I am "+os.environ['sender'], "counter":counter}
    #     msg_bytes = json.dumps(msg).encode()
    #     return msg_bytes

    # def send_msg(self,i):
    #     print("started send_msg function")
    #     while True:
    #         i+=1
    #         node1_msg_bytes = self.create_msg(i)
    #         self.UDP_Socket.sendto(node1_msg_bytes, (os.environ['target1'], 5555))
    #         self.UDP_Socket.sendto(node1_msg_bytes, (os.environ['target2'], 5555))
    #         if i>10:
    #             break
    #         # Timer(90, self.send_msg,args=[i]).start()
    #         # time.sleep(1)
        

           

        # print("All messages were sent")
        # print("Ending "+os.environ['sender'])
    def listener(self):
        print(f"Starting Listener ")
        while True:
            try:
                msg, addr = self.UDP_Socket.recvfrom(1024)
            except:
                print(f"ERROR while fetching from socket : {traceback.print_exc()}")

            # Decoding the Message received from Node 1
            decoded_msg = json.loads(msg.decode('utf-8'))
            while self.timeout>0:
                if len(msg)!=0:
                    print(msg)
                    self.timeout=int(os.environ['timeout'])
                    self.process_msgs(decoded_msg)
                else:
                    self.timeout-=1
            # self.process_msgs(decoded_msg)
            
            # print(f"Message Received : {decoded_msg} From : {addr}")

            # if decoded_msg['counter']>=10:
            #     break

        print("Exiting Listener Function")

    # Heartbeats
    def appendRPC(self,msg):
        # print(os.environ['leader'])
        # if self.currentState=='leader':
        # print(msg,type(msg) ," in appendRPC")
        if self.currentState=="leader":
            # time.sleep(30)
            while True:
                msg1 = {"msg": f"Hi, I am "+os.environ['sender'],"heartbeat":self.heartbeat,"timeout":self.timeout}
                msg_bytes = json.dumps(msg1).encode()
                try:
                    self.UDP_Socket.sendto(msg_bytes, (os.environ['target1'], 5555))
                    self.UDP_Socket.sendto(msg_bytes, (os.environ['target2'], 5555))
                except:
                     print(f"ERROR while fetching from socket : {traceback.print_exc()}")
                # if i>10:
                #     break
                # Timer(100, self.appendRPC,args=[i]).start()
            # time.sleep(1)
        elif self.currentState=="candidate" and ('votedFor' not in msg):
            # print(msg)
            msg_bytes = json.dumps(msg).encode()
            try:
                self.UDP_Socket.sendto(msg_bytes, (os.environ['target1'], 5555))
                self.UDP_Socket.sendto(msg_bytes, (os.environ['target2'], 5555))
            except:
                 print(f"ERROR while fetching from socket : {traceback.print_exc()}")
        
        elif self.currentState=="candidate" and 'votedFor' in msg:
            print(msg,"sending vote")
            msg_bytes = json.dumps(msg).encode()
            try:
                self.UDP_Socket.sendto(msg_bytes, (msg['votedFor'], 5555))
            except:
                 print(f"ERROR while fetching from socket : {traceback.print_exc()}")
            # print(self.currentState,self.vote)
        elif self.currentState=="candidate" and ('Elected_leader' in msg):
            # print(msg)
            try:
                self.UDP_Socket.sendto(msg_bytes, (os.environ['target1'], 5555))
                self.UDP_Socket.sendto(msg_bytes, (os.environ['target2'], 5555))
            except:
                 print(f"ERROR while fetching from socket : {traceback.print_exc()}")
        # else:
        #     self.process_msgs({})
        return

    def process_msgs(self,msg):
        print(msg)
        # print(self.timeout,msg)
        while self.timeout>0:
            if len(msg)!=0:
                self.timeout=int(os.environ['timeout'])
                print(msg)
            else:
                self.timeout-=1
                # self.listener()
                
        if self.timeout==0 and 'Term' not in msg:
            print("changing state to candidate")
            self.currentTerm+=1
            self.currentState="candidate"
            self.vote+=1
            msg={"Term":self.currentTerm,"candidateId":self.candidateId,"lastLogIndex":0,"lastLogTerm":0}
            self.appendRPC(msg)
        
        # print(self.currentTerm)

        if ('Term'in msg and msg['Term']>self.currentTerm-1) and self.currentState!="leader" and not self.voted:
            print("voting phase")
            self.votedFor=msg['candidateId']
            msg={"votedFor":self.votedFor}
            self.voted=True
            self.appendRPC(msg)

        if 'votedFor' in msg and msg['votedFor']==self.candidateId and len(msg)>0:
            print("counting votes")
            self.vote+=1
            if self.vote>2:
                self.currentState="leader"
                self.timeout=int(os.environ['timeout'])
                msg={'Elected_leader':self.candidateId}
                self.appendRPC(msg)
            else: 
                self.appendRPC({})
        if 'Elected_leader' in msg and len(msg)>0:
            print("elected node")
            self.currentState="follower"
            self.timeout=int(os.environ['timeout'])
        return
        # if 'sender_name' in msg and msg['sender_name']=='Controller':
        #     self.currentState="Follower"

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
    
