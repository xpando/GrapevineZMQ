using System;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using ZeroMQ;

namespace Grapevine.Core
{
    public sealed class GrapevineReceiver : IDisposable
    {
        readonly EventLoopScheduler  _scheduler;
        readonly IObservable<object> _messages;
        readonly ZmqSocket           _socket = null;
        readonly TimeSpan            _pollInterval = TimeSpan.FromMilliseconds(10);

        public GrapevineReceiver(ZmqContext context, string address, IMessageSerializer serializer)
        {
            _scheduler = new EventLoopScheduler();
            _socket    = context.CreateSocket(SocketType.SUB);
            _messages  = ListenForMessages(context, address, serializer);
        }

        IObservable<object> ListenForMessages(ZmqContext context, string address, IMessageSerializer serializer)
        {
            return 
                Observable.Create<object>
                (
                    o =>
                    {
                        _socket.Connect(address);

                        var poller = _scheduler.SchedulePeriodic
                        (
                            _pollInterval,
                            () =>
                            {
                                try
                                {
                                    var msg = _socket.ReceiveMessage(TimeSpan.Zero);
                                    while (msg != null && msg.FrameCount == 3)
                                    {
                                        var typeName = ZmqContext.DefaultEncoding.GetString(msg[1].Buffer);
                                        var type     = MessageTypeRegistry.Resolve(typeName);
                                        var data     = msg[2].Buffer;

                                        if (type != null)
                                        {
                                            var poco = serializer.Deserialize(data, type);
                                            o.OnNext(poco);
                                        }

                                        msg = _socket.ReceiveMessage(TimeSpan.Zero);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    o.OnError(ex);
                                }
                            }
                        );

                        return new CompositeDisposable(poller, Disposable.Create(() => _socket.Disconnect(address)));
                    }
                )
                .SubscribeOn(_scheduler)
                .ObserveOn(_scheduler)
                .Publish()
                .RefCount();
        }

        public IObservable<object> Messages { get { return _messages; } }

        public void AddTopic(string topic)
        {
            _scheduler.Schedule(() => _socket.Subscribe(ZmqContext.DefaultEncoding.GetBytes(topic)));
        }

        public void RemoveTopic(string topic)
        {
            _scheduler.Schedule(() => _socket.Unsubscribe(ZmqContext.DefaultEncoding.GetBytes(topic)));
        }

        public void Dispose()
        {
            _scheduler.Dispose();
            if (_socket != null)
                _socket.Dispose();
        }
    }
}
