using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZMQ;

namespace Grapevine.Client
{
    /// <summary>
    /// 
    /// </summary>
    public interface IGrapevineClient
    {
        /// <summary>
        /// Publishes a message to the Grapevine server.
        /// </summary>
        void Send<MessageType>(MessageType message);

        /// <summary>
        /// Creates an observable listener for the specified message type. 
        /// No message filtering is performed on the server.
        /// 
        /// NOTE: call Subscribe on the returned observable to receive messages
        /// and dont forget to dispose your subscription when you are done.
        /// 
        /// If you are not using the Rx framework then be sure to add a 
        /// using MaryKay.Grapevine.Extensions to make calling Subscribe
        /// on the observable much easier.
        /// </summary>
        IObservable<MessageType> Receive<MessageType>();

        /// <summary>
        /// Creates an observable listener for the specified message type
        /// and filters messages on the server using the provided filter
        /// expression.
        /// 
        /// NOTE: call Subscribe on the returned observable to receive messages
        /// and dont forget to dispose your subscription when you are done.
        /// 
        /// If you are not using the Rx framework then be sure to add a 
        /// using MaryKay.Grapevine.Extensions to make calling Subscribe
        /// on the observable much easier.
        /// </summary>
        IObservable<MessageType> Receive<MessageType>(Expression<Func<MessageType,bool>> filter);
    }

    public sealed class ObservableDispatcher : IConnectableObservable<object>, IDisposable
    {
        HashSet<string> _topics = new HashSet<string>();
        Subject<object> _messages = new Subject<object>();
        Context _context;
        string _address;
        Socket _socket;
        IDisposable _connection;

        public ObservableDispatcher(Context context, string address)
        {
            _context = context;
            _address = address;
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
            var message = _socket.RecvProtoBuf();
            if (message != null)
                _messages.OnNext(message);
        }
    }

    public sealed class GrapevineClient : IGrapevineClient, IDisposable
    {
        readonly Context _context = new Context();

        Socket _pubSocket;
        ObservableDispatcher _messages;

        public GrapevineClient(string pubAddress, string subAddress)
        {
            _pubSocket = _context.Socket(SocketType.PUSH);
            _pubSocket.Connect(pubAddress);

            _messages = new ObservableDispatcher(_context, subAddress);
            _messages.Connect();
        }

        void IGrapevineClient.Send<MessageType>(MessageType message)
        {
            _pubSocket.SendProtoBuf<MessageType>(message);
        }

        public IObservable<MessageType> Receive<MessageType>()
        {
            MessageTypeRegistry.Register<MessageType>();
            var typeName = MessageTypeRegistry.GetTypeName(typeof(MessageType));
            _messages.AddTopic(typeName);
            return _messages.OfType<MessageType>();
        }

        public IObservable<MessageType> Receive<MessageType>(Expression<Func<MessageType,bool>> filter)
        {
            return Receive<MessageType>().Where(filter.Compile());
        }

        public void Dispose()
        {
            _messages.Dispose();
            _pubSocket.Dispose();
 	        _context.Dispose();
        }
    }
}