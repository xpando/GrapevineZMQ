using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;
using ZeroMQ.Sockets;

namespace Grapevine.Core
{
    public sealed class GrapevineReceiver : IConnectableObservable<object>, IDisposable
    {
        HashSet<string> _topics = new HashSet<string>();
        Subject<object> _messages = new Subject<object>();
        IZmqContext _context;
        string _address;
        IMessageSerializer _serializer;
        ISubscribeSocket _socket;
        IDisposable _connection;

        public GrapevineReceiver(IZmqContext context, string address, IMessageSerializer serializer)
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
                    _socket.Subscribe(Encoding.Unicode.GetBytes(topic));
            }
        }

        public void RemoveTopic(string topic)
        {
            if (_topics.Contains(topic))
            {
                _topics.Remove(topic);
                if (_connection != null)
                    _socket.Unsubscribe(Encoding.Unicode.GetBytes(topic));
            }
        }

        public IDisposable Connect()
        {
            _socket = _context.CreateSubscribeSocket();
            _socket.ReceiveTimeout = TimeSpan.FromSeconds(1);
            _socket.Connect(_address);
            _socket.ReceiveReady += OnReceiveReady;

            foreach (var topic in _topics)
                _socket.Subscribe(Encoding.Unicode.GetBytes(topic));

            var cancellationTokenSource = new CancellationTokenSource();

            var task = Task.Factory.StartNew
            (
                () =>
                {
                    var pollSet = _context.CreatePollSet(new [] { _socket });
                    while (!cancellationTokenSource.IsCancellationRequested)
                        pollSet.Poll(TimeSpan.FromSeconds(1));
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
                        _socket.Unsubscribe(Encoding.Unicode.GetBytes(topic));

                    cancellationTokenSource.Cancel();
                    task.Wait();

                    _socket.ReceiveReady -= OnReceiveReady;
                    _socket.Dispose();
                    _socket = null;
                }
            );

            return _connection;
        }

        void OnReceiveReady(object sender, ReceiveReadyEventArgs e)
        {
            var typeName = Encoding.Unicode.GetString(e.Socket.Receive());
            var data = e.Socket.Receive();

            var type = MessageTypeRegistry.Resolve(typeName);
            if (type != null)
            {
                var message = _serializer.Deserialize(data, type);
                _messages.OnNext(message);
            }
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
    }
}
