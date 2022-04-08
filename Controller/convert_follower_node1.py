import json
import socket
import traceback
import time
import threading
import os

# Wait following seconds below sending the controller request


class Control:
    def __init__(self,UDP_Socket,) -> None:
        
        self.UDP_Socket=UDP_Socket
        

    def convert_follower_node1(self, msg_1) -> None:
# Read Message Template
        #time.sleep(20)
        msg = json.load(open("Message.json"))

        # Initialize
        sender = "Controller"
        if 'Elected_leader' in msg_1:
            target = msg_1['Elected_leader']
        else:
            target = ""
        port = 5555

        # Request
        msg['sender_name'] = sender
        msg['request'] = "CONVERT_FOLLOWER"
        print(f"Request Created : {msg}")

        # Socket Creation and Binding
        # skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        # skt.bind((sender, port))
        start_time = time.time()
        #print(str(start_time))

        # Send Message
        while(time.time()<(start_time + 20)):
            a=1
            #print(str(time.time()))
        print("infinite loop passed")

        try:
            # Encoding and sending the message
            if target!="":
                self.UDP_Socket.sendto(json.dumps(msg).encode('utf-8'), (target, port))
                print("message sent")
                #start_time = time.time()
        except:
            #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
            print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")
        print("Convert Follower Controller Ended")

    def listener(self):
        print(f"Starting Listener ")
        # while True:
        #start_time=time.time()
        decoded_msg={}
        while True:
            try:
                msg, addr = self.UDP_Socket.recvfrom(1024)
                #start_time=time.time()
            except:
                print(f"ERROR while fetching from socket : {traceback.print_exc()}")
            decoded_msg = json.loads(msg.decode('utf-8'))
            #print(decoded_msg['Elected Leader'])
            print("listner ",decoded_msg)
            print("Leader is ",decoded_msg['Elected_leader'])
            self.convert_follower_node1(decoded_msg)
        
        print("Listener Ended")

if __name__ == "__main__":

    
    # Creating Socket and binding it to the target container IP and port
    UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    # Bind the node to sender ip and port
    UDP_Socket.bind((os.environ['sender'], 5555))

    # Sending 5 messages to Node 2
    # for i in range(5):
    control = Control(UDP_Socket)

    threading.Thread(target=control.listener).start()
    threading.Thread(target=control.convert_follower_node1,args=[{}]).start()
    #Control.convert_follower_node1()



