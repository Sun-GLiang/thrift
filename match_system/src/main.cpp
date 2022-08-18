// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "match_server/Match.h"
#include "save_client/Save.h"
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/TToString.h>


#include <iostream>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using namespace  ::match_service;
using namespace  ::save_service;

using namespace std;

struct Task
{
    User user;
    string type;
};

struct MessageQueue
{
    queue<Task> q;
    mutex m;
    condition_variable cv;
}message_queue;


class Pool
{
    public:
        void save_result(int a, int b)
        {
            printf("匹配结果为 %d %d\n", a, b);


            // 存储数据的服务器的IP地址及端口号
            std::shared_ptr<TTransport> socket(new TSocket("123.57.47.211", 9090));
            std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
            std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
            SaveClient client(protocol);

            try {
                transport->open();

                client.save_data("acs_5946", "f9f9a39b", a, b);

                transport->close();
            } catch (TException& tx) {
                cout << "ERROR: " << tx.what() << endl;
            }
        }

        bool check_match(uint32_t i, uint32_t j)
        {
            auto a = users[i], b = users[j];

            int delt = abs(a.score - b.score); // 分差
            int a_max_diff = wt[i] * 50;
            int b_max_diff = wt[j] * 50;

            return delt <= a_max_diff && delt <= b_max_diff;
        }

        void match()
        {
            // 等待秒数+1
            for (uint32_t i = 0; i < wt.size(); i++)
                wt[i]++;

            while (users.size() > 1)
            {
                bool flag = true;
                for (uint32_t i = 0; i < users.size(); i++)
                {
                    for (uint32_t j = i + 1; j < users.size(); j++)
                    {
                        if (check_match(i, j))
                        {
                            auto a = users[i], b = users[j];
                            users.erase(users.begin() + j);
                            users.erase(users.begin() + i);
                            wt.erase(wt.begin() + j);
                            wt.erase(wt.begin() + i);
                            save_result(a.id, b.id);

                            flag = false;
                            break;
                        }

                        if (!flag) break;
                    }
                }
                if (flag) break;
            }
        }

        void add(User user)
        {
            users.push_back(user);
            wt.push_back(0);
        }

        void remove(User user)
        {
            for (uint32_t i = 0; i < users.size(); i++)
                if (users[i].id == user.id)
                {
                    users.erase(users.begin() + i);
                    wt.erase(wt.begin() + i);
                    break;
                }
        }

        // 存储所有的玩家
    private:
        vector<User> users;
        vector<int> wt; // 用户等待时间, 单位s
}pool;



class MatchHandler : virtual public MatchIf {
    public:
        MatchHandler() {
            // Your initialization goes here
        }

        int32_t add_user(const User& user, const std::string& info) {
            // Your implementation goes here
            printf("add_user\n");

            // 加锁 不需要显式将其解锁 当这个局部执行完后即该局部变量就会消失，会自动解锁掉
            unique_lock<mutex> lck(message_queue.m);

            message_queue.q.push({user, "add"});

            // 唤醒条件变量 通知所有被条件变量卡住的线程，会有一个随机的线程执行
            message_queue.cv.notify_all();

            return 0;
        }

        int32_t remove_user(const User& user, const std::string& info) {
            // Your implementation goes here
            printf("remove_user\n");

            // 加锁
            unique_lock<mutex> lck(message_queue.m);

            message_queue.q.push({user, "remove"});

            // 唤醒条件变量
            message_queue.cv.notify_all();

            return 0;
        }

};

class MatchCloneFactory : virtual public MatchIfFactory {
    public:
        ~MatchCloneFactory() override = default;
        MatchIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) override
        {
            std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(connInfo.transport);
            /*
            cout << "Incoming connection\n";
            cout << "\tSocketInfo: "  << sock->getSocketInfo() << "\n";
            cout << "\tPeerHost: "    << sock->getPeerHost() << "\n";
            cout << "\tPeerAddress: " << sock->getPeerAddress() << "\n";
            cout << "\tPeerPort: "    << sock->getPeerPort() << "\n";
            */
            return new MatchHandler;
        }
        void releaseHandler(MatchIf* handler) override {
            delete handler;
        }
};

void consume_task()
{
    while (true)
    {
        unique_lock<mutex> lck(message_queue.m);
        if (message_queue.q.empty())
        {
            // 当一个游戏刚开始时队列一般都是空的，若continue的话，则consume_task会陷入死循环，不停地占用锁，CPU占有率很大
            // 可以考虑当发现队列为空，就把这个进程阻塞住，把它卡住，直到有新的玩家加入
            // 用条件变量，先将这个锁解开，然后就把这个线程卡在这里
            // 直到其他线程将这个条件变量唤醒为止
            // message_queue.cv.wait(lck);

            // 当队列是空的，则解锁，放弃消费线程对锁的把控，紧接着睡眠1秒钟，而后进行匹配（note：在这睡眠的1秒钟内其他线程可以对消息队列进行操作，可能会匹配成功）
            lck.unlock();
            sleep(1);
            pool.match();
        }
        else
        {
            auto task = message_queue.q.front();
            message_queue.q.pop();
            // 解锁 保证都后面do task时也可以支持添加/删除用户的操作
            // 处理完共享的变量后要及时解锁
            lck.unlock();

            // do task
            // 将所有的用户放到匹配池进行匹配
            if (task.type == "add") pool.add(task.user);
            else if (task.type == "remove") pool.remove(task.user);
        }
    }
}


int main(int argc, char **argv) {
    TThreadedServer server(
            std::make_shared<MatchProcessorFactory>(std::make_shared<MatchCloneFactory>()),
            std::make_shared<TServerSocket>(9090), //port
            std::make_shared<TBufferedTransportFactory>(),
            std::make_shared<TBinaryProtocolFactory>());

    cout << "开始匹配服务" << endl;

    thread matching_thread(consume_task);

    server.serve();
    return 0;
}

