from match_client.match import Match
from match_client.match.ttypes import User

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# 用来从终端读取信息的包
from sys import stdin

def operate(op, user_id, user_name, score):
    # Make socket
    transport = TSocket.TSocket('localhost', 9090)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = Match.Client(protocol)

    # Connect!
    transport.open()

    #
    user = User(user_id, user_name, score)

    # 对操作进行判断添加用户还是删除用户
    if op == "add":
        client.add_user(user, "")
    elif op == "remove":
        client.remove_user(user, "")

    # Close!
    transport.close()

def main():
    # 读入信息
    for line in stdin:
        op, user_id, user_name, score = line.split(' ')
        operate(op, int(user_id), user_name, int(score))


if __name__ == "__main__":
    main()
