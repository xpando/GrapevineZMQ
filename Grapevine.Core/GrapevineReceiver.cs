using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;

namespace Grapevine.Core
{
    public sealed class GrapevineReceiver : IConnectableObservable<object>, IDisposable
    {
        HashSet<string> _topics = new HashSet<string>();
        Subject<object> _messages = new Subject<object>();
        ZmqContext _context;
        string _pubAddress;
        IMessageSerializer _serializer;
        ZmqSocket _socket;
        IDisposable _connection;

        public GrapevineReceiver(ZmqContext context, string pubAddress, IMessageSerializer serializer)
        {
            _context    = context;
            _pubAddress = pubAddress;
            _serializer = serializer;
        }

        public void AddTopic(string topic)
        {
            if (!_topics.Contains(topic))
            {
                _topics.Add(topic);
                if (_connection != null)
                    _socket.Subscribe(ZmqContext.DefaultEncoding.GetBytes(topic));
            }
        }

        public void RemoveTopic(string topic)
        {
            if (_topics.Contains(topic))
            {
                _topics.Remove(topic);
                if (_connection != null)
                    _socket.Unsubscribe(ZmqContext.DefaultEncoding.GetBytes(topic));
            }
        }

        public IDisposable Connect()
        {
            _socket = _context.CreateSocket(SocketType.SUB);
            _socket.Connect(_pubAddress);

            foreach (var topic in _topics)
                _socket.Subscribe(ZmqContext.DefaultEncoding.GetBytes(topic));

            var cancellationTokenSource = new CancellationTokenSource();

            var task = Task.Factory.StartNew
            (
                () =>
                {
                    _socket.ReceiveReady += Dispatch;

                    var poller = new Poller(new[] { _socket });
                    while (!cancellationTokenSource.IsCancellationRequested)
                        poller.Poll(TimeSpan.FromSeconds(1));

                    _socket.ReceiveReady -= Dispatch;
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
                        _socket.Unsubscribe(ZmqContext.DefaultEncoding.GetBytes(topic));

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

        void Dispatch(object sender, SocketEventArgs e)
        {
            var msg      = e.Socket.ReceiveMessage();
            var typeName = ZmqContext.DefaultEncoding.GetString(msg[1].Buffer);
            var data     = msg[2].Buffer;

            var type = MessageTypeRegistry.Resolve(typeName);
            if (type != null)
            {
                var message = _serializer.Deserialize(data, type);
                _messages.OnNext(message);
            }
        }
    }
}
