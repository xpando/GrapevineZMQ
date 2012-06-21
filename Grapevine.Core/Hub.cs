using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using ZMQ;

namespace Grapevine.Core
{
    public class Hub
    {
        static readonly Logger _logger = LogManager.GetCurrentClassLogger();

        Context _context;
        string _pubAddress;
        string _pullAddress;
        Socket _pubSocket;
        Socket _pullSocket;
        CancellationTokenSource _tokenSource;
        TaskFactory _taskFactory = new TaskFactory();
        Task _task;

        public Hub(Context context, string pubAddress, string pullAddress)
        {
            _context     = context;
            _pubAddress  = pubAddress;
            _pullAddress = pullAddress;
        }

        public void Start()
        {
            if (_task != null)
                throw new InvalidOperationException("Hub is already started.");

            _tokenSource = new CancellationTokenSource();

            _pubSocket = _context.Socket(SocketType.PUB);
            _pullSocket = _context.Socket(SocketType.PULL);

            _pubSocket.Bind(_pubAddress);
            _pullSocket.Bind(_pullAddress);

            var pollItems = new PollItem[1];
            pollItems[0] = _pullSocket.CreatePollItem(IOMultiPlex.POLLIN);
            pollItems[0].PollInHandler += Forward;

            _task = _taskFactory.StartNew
            (
                () =>
                {
                    while (!_tokenSource.IsCancellationRequested)
                    {
                        _context.Poll(pollItems, 1000000);
                    }
                }, 
                TaskCreationOptions.LongRunning
            );
        }

        public void Stop()
        {
            _tokenSource.Cancel();

            _task.Wait();

            _pullSocket.Dispose();
            _pubSocket.Dispose();

            _pullSocket = null;
            _pubSocket = null;
            _task = null;
        }

        void Forward(Socket socket, IOMultiPlex revents)
        {
            var typeName = socket.Recv(Encoding.UTF8);
            var data = socket.Recv();

            //_logger.Info("Forwarding:\r\n\tType: {0}\r\n\tMessage: {1}", typeName, data);

            _pubSocket.Send(typeName, Encoding.UTF8, SendRecvOpt.SNDMORE);
            _pubSocket.Send(data);
        }
    }
}
