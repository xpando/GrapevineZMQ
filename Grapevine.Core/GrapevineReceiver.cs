using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZMQ;

namespace Grapevine.Core
{
    public sealed class GrapevineReceiver
    {
        HashSet<string> _topics = new HashSet<string>();
        Context _context;
        string _address;
        IMessageSerializer _serializer;
        IDisposable _connection;
        IObservable<object> _pipe = null;
        Action<string, Encoding> _addTopicHandler = null;
        Action<string, Encoding> _removeTopicHandler = null;

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
                    _addTopicHandler(topic, Encoding.Unicode);
            }
        }

        public void RemoveTopic(string topic)
        {
            if (_topics.Contains(topic))
            {
                _topics.Remove(topic);
                if (_connection != null)
                    _removeTopicHandler(topic, Encoding.Unicode);
            }
        }

        public IObservable<object> Receive()
        {

            _pipe = _pipe ?? Observable.Defer(() => Observable.Create<object>(o =>
                    {
                        var cancel = new CancellationDisposable();

                        Scheduler.NewThread.Schedule(() =>
                        {
                            Socket socket = _context.Socket(SocketType.SUB);

                            _addTopicHandler = (t, e) => socket.Subscribe(t, e);
                            _removeTopicHandler = (t, e) => socket.Unsubscribe(t, e);
                            
                            socket.Connect(_address);

                            foreach (var topic in _topics)
                                socket.Subscribe(topic, Encoding.Unicode);

                            var pollItems = new PollItem[1];
                            pollItems[0] = socket.CreatePollItem(IOMultiPlex.POLLIN);

                            ZMQ.PollHandler act = (s, r) => Dispatch(o, s, r);

                            pollItems[0].PollInHandler += act;

                            while (!cancel.Token.IsCancellationRequested)
                                _context.Poll(pollItems, 2000);

                            pollItems[0].PollInHandler -= act;
                            socket.Dispose();
                            o.OnCompleted();

                        }
                        );

                        return cancel;
                    }))
                    .Publish()
                    .RefCount();

            return _pipe;
        }



        void Dispatch(IObserver<object> destination, Socket socket, IOMultiPlex revents)
        {
            var typeName = socket.Recv(Encoding.Unicode);
            var data = socket.Recv();

            var type = MessageTypeRegistry.Resolve(typeName);
            if (type != null)
            {
                var message = _serializer.Deserialize(data, type);
                destination.OnNext(message);
            }
        }
    }
}
