using System;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Reactive.Subjects;
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

    public sealed class ObservableSocket<T> : IConnectableObservable<T>, IDisposable
    {
        readonly Context _context;
        readonly string _address;
        readonly SocketType _socketType;
        readonly Subject<T> _messages = new Subject<T>();
        CancellationTokenSource _cancellationTokenSource;
        Task _task;
        Socket _socket;

        public ObservableSocket(Context context, SocketType socketType, string address)
        {
            _context    = context;
            _address    = address;
            _socketType = socketType;
        }

        public IDisposable Connect()
        {
            _socket = _context.Socket(_socketType);
            _socket.Connect(_address);

            _cancellationTokenSource = new CancellationTokenSource();

            _task = Task.Factory.StartNew
            (
                () =>
                {
                    while (!_cancellationTokenSource.IsCancellationRequested)
                    {
                        var message = _socket.RecvProtoBuf();
                        if (message is T)
                            _messages.OnNext((T)message);
                    }
                },
                TaskCreationOptions.LongRunning
            );

            return this;
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            return _messages.Subscribe(observer);
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _task.Wait();
            _socket.Dispose();
        }
    }

    public sealed class GrapevineClient : IGrapevineClient, IDisposable
    {
        readonly Context _context = new Context();

        Socket _pubSocket;
        ObservableSocket<object> _messages;

        public GrapevineClient(string pubAddress, string subAddress)
        {
            _pubSocket = _context.Socket(SocketType.PUSH);
            _pubSocket.Connect(pubAddress);

            _messages = new ObservableSocket<object>(_context, SocketType.SUB, subAddress);
            _messages.Connect();
        }

        void IGrapevineClient.Send<MessageType>(MessageType message)
        {
            _pubSocket.SendProtoBuf<MessageType>(message);
        }

        IObservable<MessageType> IGrapevineClient.Receive<MessageType>()
        {
            return _messages.OfType<MessageType>();
        }

        IObservable<MessageType> IGrapevineClient.Receive<MessageType>(Expression<Func<MessageType,bool>> filter)
        {
            return _messages
                .OfType<MessageType>()
                .Where(filter.Compile());
        }

        public void Dispose()
        {
            _pubSocket.Dispose();
 	        _context.Dispose();
        }
    }
}