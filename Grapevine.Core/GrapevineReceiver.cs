using System;
using System.Collections.Concurrent;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using NLog;
using ZeroMQ;

namespace Grapevine.Core
{
    public interface IGrapevineReceiver
    {
        void AddTopic(string topic);
        void RemoveTopic(string topic);

        IObservable<object> Messages { get; }
    }

    public interface IGrapevineReceiverFactory
    {
        IGrapevineReceiver Create(string address);
    }

    public sealed class GrapevineReceiverFactory : IGrapevineReceiverFactory
    {
        IZmqContext _context;
        IMessageSerializer _serializer;
        ConcurrentDictionary<string, IGrapevineReceiver> _receivers = new ConcurrentDictionary<string, IGrapevineReceiver>();

        public GrapevineReceiverFactory(IZmqContext context, IMessageSerializer serializer)
	    {
            _context = context;
            _serializer = serializer;
	    }

        public IGrapevineReceiver Create(string address)
        {
            var receiver = _receivers.GetOrAdd(address.Trim().ToLower(), new GrapevineReceiver(address, _context, _serializer));
            return receiver;
        }
    }

    public sealed class GrapevineReceiver : IGrapevineReceiver
    {
        static readonly Logger _logger = LogManager.GetCurrentClassLogger();

        IObservable<object> _messages;
        static Subject<string> _addTopics = new Subject<string>();
        static Subject<string> _removeTopics = new Subject<string>();

        internal GrapevineReceiver(string address, IZmqContext context, IMessageSerializer serializer)
        {
            _messages = Observable
                .Defer // wait until first subscriber to create a conection to the server
                (
                    () => Observable.Create<object>
                    (
                        observer =>
                        {
                            var cancel = new CancellationDisposable();

                            // use a background thread to read and dispatch incoming messages
                            Scheduler.NewThread.Schedule
                            (
                                () => DispatchMessages(context, address, serializer, observer, cancel.Token)
                            );

                            return cancel;
                        }
                    )
                )
                .Publish()    // make this observable connectable
                .RefCount();  // auto connect/dispose on first/last subscriber
        }

        public void AddTopic(string topic)
        {
            _addTopics.OnNext(topic);
        }

        public void RemoveTopic(string topic)
        {
            _removeTopics.OnNext(topic);
        }

        public IObservable<object> Messages { get { return _messages; } }

        static void DispatchMessages(IZmqContext context, string address, IMessageSerializer serializer, IObserver<object> observer, CancellationToken token)
        {
            try
            {
                using (var socket = context.CreateSubscribeSocket())
                {
                    socket.Connect(address);

                    socket.SubscribeAll();
                    // TODO: socket.Subscribe(Encoding.Unicode.GetBytes(topic));
                    // TODO: socket.Unsubscribe(Encoding.Unicode.GetBytes(topic));

                    EventHandler<ReceiveReadyEventArgs> dispatchMessage = (s,a) =>
                    {
                        var typeName = Encoding.Unicode.GetString(socket.Receive());
                        var data = socket.Receive();

                        var type = MessageTypeRegistry.Resolve(typeName);
                        if (type != null)
                        {
                            var message = serializer.Deserialize(data, type);
                            observer.OnNext(message);
                        }
                    };

                    socket.ReceiveReady += dispatchMessage;

                    var poller = context.CreatePollSet(new [] { socket });
                    while (!token.IsCancellationRequested)
                        poller.Poll(TimeSpan.FromSeconds(1));

                    socket.ReceiveReady -= dispatchMessage;

                    observer.OnCompleted();
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex);
                observer.OnError(ex);
            }
        }
    }
}
