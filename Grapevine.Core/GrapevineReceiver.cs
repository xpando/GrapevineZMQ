using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZMQ;

namespace Grapevine.Core
{
    public sealed class GrapevineReceiver : IConnectableObservable<object>, IDisposable
    {
        HashSet<string> _topics = new HashSet<string>();
        Subject<object> _messages = new Subject<object>();
        Context _context;
        string _address;
        IMessageSerializer _serializer;
        Socket _socket;
        IDisposable _connection;

        public GrapevineReceiver(Context context, string address, IMessageSerializer serializer)
        {
            _context = context;
            _address = address;
            _serializer = serializer;
        }

        public void AddTopic(string topic)
        {
            if (!_topics.Contains(topic))
            {
                _topics.Add(topic);
                if (_connection != null)
                    _socket.Subscribe(topic, Encoding.Unicode);
            }
        }

        public void RemoveTopic(string topic)
        {
            if (_topics.Contains(topic))
            {
                _topics.Remove(topic);
                if (_connection != null)
                    _socket.Unsubscribe(topic, Encoding.Unicode);
            }
        }

        public IDisposable Connect()
        {
            _socket = _context.Socket(SocketType.SUB);
            _socket.Connect(_address);

            foreach (var topic in _topics)
                _socket.Subscribe(topic, Encoding.Unicode);

            var cancellationTokenSource = new CancellationTokenSource();

            var task = Task.Factory.StartNew
            (
                () =>
                {
                    var pollItems = new PollItem[1];
                    pollItems[0] = _socket.CreatePollItem(IOMultiPlex.POLLIN);
                    pollItems[0].PollInHandler += Dispatch;

                    while (!cancellationTokenSource.IsCancellationRequested)
                        _context.Poll(pollItems, 1000000);

                    pollItems[0].PollInHandler -= Dispatch;
                },
                TaskCreationOptions.LongRunning
            );

            if (_connection != null)
                _connection.Dispose();

            _connection = Disposable.Create
            (
                () =>
                {
                    _connection = null;

                    foreach (var topic in _topics)
                        _socket.Unsubscribe(topic, Encoding.Unicode);

                    cancellationTokenSource.Cancel();
                    task.Wait();

                    _socket.Dispose();
                    _socket = null;
                }
            );

            return _connection;
        }

        public IDisposable Subscribe(IObserver<object> observer)
        {
            return _messages.Subscribe(observer);
        }

        public void Dispose()
        {
            if (_connection != null)
                _connection.Dispose();
        }

        void Dispatch(Socket socket, IOMultiPlex revents)
        {
            var typeName = socket.Recv(Encoding.Unicode);
            var data = socket.Recv();

            var type = MessageTypeRegistry.Resolve(typeName);
            if (type != null)
            {
                var message = _serializer.Deserialize(data, type);
                _messages.OnNext(message);
            }
        }
    }
}
